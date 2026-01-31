(ns idem.dispatcher
  "Outbox Dispatcher (Relay).
   Responsible for polling pending events and publishing them via a Publisher."
  (:require [clojure.tools.logging :as log]
            [idem.protocol :as protocol]
            [idem.impl.postgres :as pg]))

;; --- Configuration Defaults ---

(def ^:private default-opts
  {:poll-interval-ms 1000
   :batch-size       50
   :max-attempts     10
   :initial-backoff-ms 1000
   :backoff-multiplier 2
   :table-name       :idem_outbox_events})

;; --- Logic ---

(defn- process-event!
  [store publisher event opts]
  (let [event-id  (:event_id event)
        attempts  (:attempts event)]
    (try
      ;; 1. Publish (outside DB transaction to avoid long tx)
      (protocol/publish! publisher event)
      
      ;; 2. Success: Mark as sent
      (protocol/mark-outbox-sent! store event-id)
      
      (catch Exception e
        (log/error e (format "Failed to publish event %s" event-id))
        ;; 3. Failure: Mark as failed (Retry or Dead)
        (try
          (protocol/mark-outbox-failed! store event-id (.getMessage e) attempts opts)
          (catch Exception db-e
            (log/error db-e "CRITICAL: Failed to update status for failed event!")))))))

(defn- process-batch!
  [store publisher opts]
  (let [events (protocol/claim-batch! store (:batch-size opts) opts)]
    (when (seq events)
      (log/debugf "Dispatcher claimed %d events" (count events))
      (doseq [event events]
        (process-event! store publisher event opts)))
    (count events)))

;; --- Lifecycle ---

(defn start!
  "Starts the Dispatcher background loop.
   
   Parameters:
     ds        - Database DataSource (used to create the default Postgres store).
     publisher - An implementation of idem.protocol/Publisher.
     opts      - Configuration map (optional):
       :poll-interval-ms    Polling interval (default 1000)
       :batch-size          Batch size (default 50)
       :max-attempts        Max retry attempts (default 10)
       :table-name          Table name (default :idem_outbox_events)

   Returns:
     A stop function (no-args). Calling it stops the background thread."
  [ds publisher & [opts]]
  (let [full-opts (merge default-opts opts)
        ;; Create the store instance (defaulting to Postgres)
        store     (pg/->dispatcher-store ds full-opts)
        running?  (atom true)
        worker    (Thread.
                   (fn []
                     (log/info "Dispatcher started with config:" full-opts)
                     (protocol/start! publisher)
                     (while @running?
                       (try
                         (let [processed-count (process-batch! store publisher full-opts)]
                           ;; If idle, sleep; otherwise continue immediately
                           (when (zero? processed-count)
                             (Thread/sleep (:poll-interval-ms full-opts))))
                         (catch InterruptedException _
                           (log/info "Dispatcher interrupted, stopping..."))
                         (catch Exception e
                           (log/error e "Error in dispatcher loop")
                           (Thread/sleep 5000))))
                     (protocol/stop! publisher)
                     (log/info "Dispatcher stopped.")))]
    (.start worker)
    (fn []
      (reset! running? false)
      (.interrupt worker))))
