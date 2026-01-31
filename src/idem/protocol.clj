(ns idem.protocol
  "Defines the core protocols for the IDEM library.
   These protocols abstract the underlying storage and messaging technologies,
   allowing for pluggable implementations (e.g., PostgreSQL, MySQL, Redis, Kafka)."
  )

;; --- 1. Outbox Protocols ---

(defprotocol OutboxStore
  "Abstraction for persisting events within a business transaction."
  
  (emit! [this tx event-map]
    "Persists an event to the outbox storage.
     
     Parameters:
       tx        - The transaction context (e.g., java.sql.Connection, Hibernate Session).
       event-map - A map containing event details:
                   {:aggregate-type String
                    :aggregate-id   String
                    :event-type     String
                    :payload        Map
                    :headers        Map (optional)}
     
     Returns:
       A map containing the generated {:event-id ...}"))

(defprotocol DispatcherStore
  "Abstraction for the Dispatcher to claim and update outbox events."
  
  (claim-batch! [this batch-size opts]
    "Claims a batch of pending (or retriable) events.
     Should handle concurrency safely (e.g., SELECT ... FOR UPDATE SKIP LOCKED).
     
     Parameters:
       batch-size - Maximum number of events to fetch.
       opts       - Implementation-specific options.
     
     Returns:
       A sequence of event maps.")
  
  (mark-outbox-sent! [this event-id]
    "Marks a specific event as successfully sent.")
  
  (mark-outbox-failed! [this event-id error-msg attempt-count opts]
    "Marks a specific event as failed.
     Depending on implementation, this might schedule a retry or mark as dead."))

;; --- 2. Inbox Protocols ---

(defprotocol InboxStore
  "Abstraction for idempotency and lease management."
  
  (acquire-lock! [this consumer-group msg-id ttl-ms]
    "Attempts to acquire a processing lock (lease) for a message.
     
     Should return true if:
     1. The message is seen for the first time.
     2. The message was seen but the previous lease expired (takeover).
     
     Parameters:
       consumer-group - Identifier for the consumer group.
       msg-id         - Unique message identifier.
       ttl-ms         - Time-to-live for the lease in milliseconds.
     
     Returns:
       true if lock acquired, false otherwise.")
  
  (mark-inbox-processed! [this consumer-group msg-id]
    "Marks the message as successfully processed (permanently).")
  
  (mark-inbox-failed! [this consumer-group msg-id error-msg]
    "Marks the message processing as failed."))

;; --- 3. Publisher Protocol ---

(defprotocol Publisher
  "Abstraction for the messaging system transport (e.g., Kafka, HTTP)."
  
  (start! [this]
    "Lifecycle: Start the publisher (e.g., open connections).")
  
  (stop! [this]
    "Lifecycle: Stop the publisher (e.g., close connections).")

  (publish! [this event]
    "Publishes a single event.
     
     Parameters:
       event - The event map from the OutboxStore.
     
     Returns:
       Implementation dependent (e.g., future, promise, or nil)."))

;; --- 4. Adapters ---

(defrecord FnPublisher [publish-fn]
  Publisher
  (start! [this] this)
  (stop! [this] this)
  (publish! [this event]
    (publish-fn event)))

(defn fn->publisher
  "Adapts a simple function (fn [event] ...) into a Publisher."
  [f]
  (->FnPublisher f))
