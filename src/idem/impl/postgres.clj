(ns idem.impl.postgres
  "PostgreSQL implementation of IDEM protocols."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [next.jdbc.result-set :as rs]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [idem.protocol :as protocol])
  (:import [org.postgresql.util PGobject]
           [java.time OffsetDateTime]
           [java.time.temporal ChronoUnit]))

;; --- Utils ---

(defn- to-jsonb
  [data]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/generate-string data))))

(defn- now-plus [ms]
  (.plus (OffsetDateTime/now) ms ChronoUnit/MILLIS))

(defn- calculate-backoff
  [attempts {:keys [initial-backoff-ms backoff-multiplier]}]
  (let [delay-ms (* initial-backoff-ms (Math/pow backoff-multiplier (dec attempts)))
        jitter   (* delay-ms (* 0.1 (rand)))
        total-ms (long (+ delay-ms jitter))]
    (now-plus total-ms)))

;; --- OutboxStore Implementation ---

(defrecord PgOutboxStore [table-name]
  protocol/OutboxStore
  
  (emit! [_ tx event-map]
    (let [{:keys [aggregate-type aggregate-id event-type payload headers]} event-map]
      (when-not (and aggregate-type aggregate-id event-type payload)
        (throw (ex-info "Missing required fields for outbox event" {:event event-map})))
      
      (let [event-id (java.util.UUID/randomUUID)
            row      {:event_id       event-id
                      :aggregate_type aggregate-type
                      :aggregate_id   (str aggregate-id)
                      :event_type     event-type
                      :payload        (to-jsonb payload)
                      :headers        (when headers (to-jsonb headers))
                      :status         "pending"
                      :attempts       0
                      :created_at     (OffsetDateTime/now)}]
        
        (log/debugf "Emitting outbox event: %s / %s" aggregate-type event-type)
        (sql/insert! tx table-name row)
        {:event-id event-id}))))

;; --- DispatcherStore Implementation ---

(defrecord PgDispatcherStore [ds table-name]
  protocol/DispatcherStore
  
  (claim-batch! [_ batch-size {:keys [max-attempts]}]
    (jdbc/with-transaction [tx ds]
      (let [now (OffsetDateTime/now)
            sql (format "SELECT * FROM %s
                         WHERE status = 'pending'
                            OR (status = 'failed' 
                                AND next_attempt_at <= ? 
                                AND attempts < ?)
                         ORDER BY created_at ASC
                         LIMIT ?
                         FOR UPDATE SKIP LOCKED"
                        (name table-name))]
        (jdbc/execute! tx [sql now max-attempts batch-size]
                       {:builder-fn rs/as-unqualified-lower-maps}))))
  
  (mark-outbox-sent! [_ event-id]
    (jdbc/with-transaction [tx ds]
      (let [sql (format "UPDATE %s 
                         SET status = 'sent', 
                             published_at = ? 
                         WHERE event_id = ?" 
                        (name table-name))]
        (jdbc/execute! tx [sql (OffsetDateTime/now) event-id]))))
  
  (mark-outbox-failed! [_ event-id error-msg attempts opts]
    (jdbc/with-transaction [tx ds]
      (let [new-attempts (inc attempts)
            max-attempts (:max-attempts opts)
            is-dead      (>= new-attempts max-attempts)
            now          (OffsetDateTime/now)]
        (if is-dead
          (do
            (log/errorf "Event %s reached max attempts (%d). Marking as DEAD. Error: %s" 
                        event-id max-attempts error-msg)
            (jdbc/execute! tx [(format "UPDATE %s 
                                        SET status = 'dead', 
                                        attempts = ?, 
                                        last_error = ?, 
                                        dead_at = ? 
                                    WHERE event_id = ?" 
                                   (name table-name))
                           new-attempts error-msg now event-id]))
          (let [next-at (calculate-backoff new-attempts opts)]
            (log/warnf "Event %s failed (attempt %d/%d). Retrying at %s. Error: %s"
                       event-id new-attempts max-attempts next-at error-msg)
            (jdbc/execute! tx [(format "UPDATE %s 
                                        SET status = 'failed', 
                                        attempts = ?, 
                                        last_error = ?, 
                                        next_attempt_at = ? 
                                    WHERE event_id = ?"
                                       (name table-name))
                               new-attempts error-msg next-at event-id])))))))

;; --- InboxStore Implementation ---

(defrecord PgInboxStore [ds table-name]
  protocol/InboxStore
  
  (acquire-lock! [_ consumer message-id ttl-ms]
    (let [locked-until (now-plus ttl-ms)
          table        (name table-name)]
      (try
        ;; 1. Try to insert (First time seen)
        (jdbc/execute! ds 
                       [(format "INSERT INTO %s (consumer, message_id, status, locked_until)
                                 VALUES (?, ?, 'processing', ?)" table)
                        consumer message-id locked-until])
        true ;; Insert successful, lock acquired
        
        (catch Exception e
          ;; 2. Insert failed (Duplicate), check for crash recovery (Takeover)
          (let [sql-takeover (format "UPDATE %s
                                      SET locked_until = ?, retry_count = retry_count + 1, last_error = 'Takeover from crash'
                                      WHERE consumer = ? 
                                        AND message_id = ? 
                                        AND status = 'processing' 
                                        AND locked_until < ?" 
                                     table)
                result (jdbc/execute! ds [sql-takeover locked-until consumer message-id (OffsetDateTime/now)])]
            (pos? (:next.jdbc/update-count (first result))))))))

  (mark-inbox-processed! [_ consumer message-id]
    (let [sql (format "UPDATE %s 
                       SET status = 'processed', processed_at = ? 
                       WHERE consumer = ? AND message_id = ?"
                      (name table-name))]
      (jdbc/execute! ds [sql (OffsetDateTime/now) consumer message-id])))

  (mark-inbox-failed! [_ consumer message-id error-msg]
    (let [sql (format "UPDATE %s 
                       SET status = 'failed', last_error = ? 
                       WHERE consumer = ? AND message_id = ?"
                      (name table-name))]
      (jdbc/execute! ds [sql error-msg consumer message-id]))))

;; --- Factory Functions ---

(defn ->outbox-store [opts]
  (->PgOutboxStore (:table-name opts :idem_outbox_events)))

(defn ->dispatcher-store [ds opts]
  (->PgDispatcherStore ds (:table-name opts :idem_outbox_events)))

(defn ->inbox-store [ds opts]
  (->PgInboxStore ds (:table-name opts :idem_inbox_messages)))