(ns idem.integration-test
  (:require [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [idem.outbox :as outbox]
            [idem.inbox :as inbox]
            [idem.dispatcher :as dispatcher]
            [idem.protocol :as protocol])
  (:import [java.util UUID]
           [java.time OffsetDateTime]
           [java.time.temporal ChronoUnit]))

(def db-spec
  {:dbtype   "postgresql"
   :dbname   "idem_test"
   :user     "postgres"
   :password "password"
   :host     "localhost"
   :port     5432})

(def ds (jdbc/get-datasource db-spec))

(defn clean-db! []
  (jdbc/execute! ds ["TRUNCATE idem_outbox_events, idem_inbox_messages"]))

(use-fixtures :each (fn [f] (clean-db!) (f)))

(deftest test-outbox-transaction-rollback
  (testing "Outbox should not contain data when transaction rolls back"
    (try
      (jdbc/with-transaction [tx ds]
        (outbox/emit! tx {:aggregate-type "test" :aggregate-id "1" :event-type "A" :payload {}})
        (throw (Exception. "Force Rollback")))
      (catch Exception _))
    
    (let [results (jdbc/execute! ds ["SELECT * FROM idem_outbox_events"])]
      (is (empty? results)))))

(deftest test-inbox-idempotency
  (testing "Inbox executes side effect only once for duplicate deliveries"
    (let [consumer "test-c"
          msg-id   (str (UUID/randomUUID))
          counter  (atom 0)
          handler  #(swap! counter inc)]
      
      ;; First attempt
      (is (= :processed (inbox/with-idempotency ds consumer msg-id {} handler)))
      (is (= 1 @counter))
      
      ;; Second attempt (Duplicate)
      (is (= :skipped (inbox/with-idempotency ds consumer msg-id {} handler)))
      (is (= 1 @counter)))))

(deftest test-inbox-lease-takeover
  (testing "Inbox allows takeover/recovery after lease expiration (stuck processing)"
    (let [consumer "test-c"
          msg-id   (str (UUID/randomUUID))
          short-ttl {:ttl-ms 1} ;; Expires in 1ms
          counter  (atom 0)
          handler  #(swap! counter inc)]
      
      ;; 1. Simulate 'stuck' processing (Insert a row with status=processing)
      (jdbc/execute! ds ["INSERT INTO idem_inbox_messages (consumer, message_id, status, locked_until)
                          VALUES (?, ?, 'processing', ?)"
                         consumer msg-id (OffsetDateTime/now)])
      
      ;; 2. Wait for lease to expire
      (Thread/sleep 10)
      
      ;; 3. Try processing again. Should detect expired lease and takeover.
      (is (= :processed (inbox/with-idempotency ds consumer msg-id short-ttl handler)))
      (is (= 1 @counter))
      
      ;; 4. Verify DB status is updated to processed
      (let [row (jdbc/execute-one! ds ["SELECT status, retry_count FROM idem_inbox_messages WHERE consumer = ? AND message_id = ?" 
                                       consumer msg-id])]
        (is (= "processed" (:idem_inbox_messages/status row)))
        (is (= 1 (:idem_inbox_messages/retry_count row)))))))