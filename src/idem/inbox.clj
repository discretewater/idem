(ns idem.inbox
  "Inbox Core Module: Handles idempotent consumption and crash recovery.
   
   Delegates state management to an `InboxStore` (defaulting to Postgres)."
  (:require [clojure.tools.logging :as log]
            [idem.protocol :as protocol]
            [idem.impl.postgres :as pg]))

(def ^:private default-opts
  {:ttl-ms 300000 ; 5 minutes
   :table-name :idem_inbox_messages})

(defn with-idempotency
  "Inbox Idempotency Wrapper.
   
   Executes the handler ONLY if the message is seen for the first time,
   or if a previous processing attempt crashed (lease expired).
   
   Parameters:
     ds         - Database DataSource (used for default Postgres store).
     consumer   - Consumer Group ID (String).
     message-id - Unique Message ID (String).
     opts       - Config (optional):
       :ttl-ms     Lease duration in ms (default 300000).
       :table-name Table name (default :idem_inbox_messages).
     handler    - No-arg function containing business logic.
   
   Returns:
     :processed - Successfully executed.
     :skipped   - Duplicate message detected and skipped.
     :failed    - Execution threw an exception."
  [ds consumer message-id opts handler]
  (let [full-opts (merge default-opts opts)
        store     (pg/->inbox-store ds full-opts)
        ttl-ms    (:ttl-ms full-opts)]
    
    (if (protocol/acquire-lock! store consumer message-id ttl-ms)
      (try
        (log/debugf "Acquired lease for msg %s/%s" consumer message-id)
        (handler)
        (protocol/mark-inbox-processed! store consumer message-id)
        :processed
        (catch Exception e
          (log/errorf e "Processing failed for msg %s/%s" consumer message-id)
          (protocol/mark-inbox-failed! store consumer message-id (.getMessage e))
          (throw e)))
      (do
        (log/debugf "Skipped duplicate/locked msg %s/%s" consumer message-id)
        :skipped))))
