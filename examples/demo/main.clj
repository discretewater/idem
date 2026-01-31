(ns demo.main
  "IDEM Minimal Demo.
   
   Demonstration Workflow:
   1. Initialize database connection.
   2. Start the Outbox Dispatcher.
   3. Simulate Producer: Emit an event.
   4. Simulate Transport: Dispatcher receives it, simulates 'network transport'.
   5. Simulate Consumer: Receives the message, deliberately 'duplicate delivery' twice.
   6. Verify: Business logic only executes once."
  (:require [next.jdbc :as jdbc]
            [idem.outbox :as outbox]
            [idem.dispatcher :as dispatcher]
            [idem.inbox :as inbox]
            [idem.protocol :as protocol]
            [clojure.tools.logging :as log])
  (:import [java.util UUID]))

;; --- Configuration ---

(def db-spec
  {:dbtype   "postgresql"
   :dbname   "idem_test"
   :user     "postgres"
   :password "password"
   :host     "localhost"
   :port     5432})

(def ds (jdbc/get-datasource db-spec))

;; --- Simulation ---

(def consumer-id "demo-service-1")

(defn process-business-logic [payload]
  (log/info ">>> [SIDE EFFECT] Processing Order:" (:order-id payload))
  (Thread/sleep 100)) ;; Simulate processing latency

(defn simulate-transport-and-consume [event]
  (let [msg-id   (str (:event_id event))
        payload  (:payload event)]
    (log/info "--- Transport: Delivering message" msg-id "---")
    
    ;; First delivery
    (log/info "Consumer: Received (Attempt 1)")
    (inbox/with-idempotency ds consumer-id msg-id {}
      #(process-business-logic payload))
    
    ;; Second delivery (Simulate duplicate)
    (log/info "Consumer: Received (Attempt 2 - Duplicate)")
    (let [result (inbox/with-idempotency ds consumer-id msg-id {}
                   #(process-business-logic payload))]
      (when (= :skipped result)
        (log/info "Consumer: Duplicate detected, skipped execution.")))))

;; --- Main ---

(defn -main [& args]
  (log/info "Starting IDEM Demo...")
  
  ;; 1. Define Publisher: Pass Outbox event directly to Consumer simulation function
  (let [publisher (protocol/fn->publisher simulate-transport-and-consume)
        stop-fn   (dispatcher/start! ds publisher {:poll-interval-ms 500})]
    
    (try
      ;; 2. Producer Emits Event
      (let [order-id (str (UUID/randomUUID))
            event    {:aggregate-type "order"
                      :aggregate-id   order-id
                      :event-type     "order.created"
                      :payload        {:order-id order-id :amount 100.0}}]
        
        (log/info "Producer: Emitting event for Order" order-id)
        (jdbc/with-transaction [tx ds]
          (outbox/emit! tx event))
        
        ;; Wait for Dispatcher to process
        (Thread/sleep 2000))
      
      (finally
        (stop-fn)
        (log/info "Demo finished.")))
    
    (System/exit 0)))