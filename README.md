# IDEM

[![Clojars Project](https://img.shields.io/clojars/v/com.github.discretewater/idem.svg)](https://clojars.org/com.github.discretewater/idem)

A lightweight, reliable **Transactional Outbox + Inbox** library for Clojure services backed by PostgreSQL.

It ensures **effectively-once** processing in microservices by solving the "Dual Write Problem" and handling idempotent consumption.

## Features

- **Outbox**: Persist events in the same transaction as your business logic.
- **Dispatcher**: Reliable delivery with `SKIP LOCKED` concurrency, retries, exponential backoff, and dead-letter handling.
- **Inbox**: Idempotent consumption using `UNIQUE` constraints and a Lease/Takeover mechanism for crash recovery.
- **Protocol-Driven**: Core logic is abstracted via Protocols (`OutboxStore`, `InboxStore`), allowing for future backend replacements (e.g., Redis, MySQL, Kafka) without changing business code.
- **Observability**: Structured logging for every dispatch and deduplication event.

## Quick Start

### Prerequisites

- Java 11+
- Clojure 1.11+
- PostgreSQL 12+ (Docker recommended)

### 1. Start Database

Start a PostgreSQL instance using Docker:

```bash
docker-compose up -d
```

### 2. Initialize Database & Schema

Create the database and apply the required tables:

```bash
# 1. Create the 'idem_test' database
python3 dev/init_db.py

# 2. Apply Schema (using docker exec)
docker-compose exec -T postgres psql -U postgres -d idem_test < resources/migrations/001_create_outbox.sql
docker-compose exec -T postgres psql -U postgres -d idem_test < resources/migrations/002_create_inbox.sql
```

### 3. Run the Demo

Run the integrated demo which simulates a Producer, a Relay, and a Consumer (with duplicate delivery):

```bash
clojure -M:demo
```

**Expected Output:**
You should see logs indicating:
1.  An event being emitted ("Producer: Emitting event...").
2.  The Dispatcher claiming and sending the event.
3.  The Consumer processing the first attempt (Side Effect executed).
4.  The Consumer receiving a duplicate (Attempt 2) and skipping it ("Duplicate detected, skipped execution").

---

## Usage Guide

IDEM is designed with a **Protocol-First** architecture. While it ships with a production-ready PostgreSQL implementation, the public APIs are decoupled from the storage layer.

### 1. Installation

Add the library to your `deps.edn`:

```clojure
{:deps {com.github.discretewater/idem {:mvn/version "0.1.1"}}}
```

Or `project.clj` (Leiningen):

```clojure
[com.github.discretewater/idem "0.1.1"]
```

### 2. Producer (Outbox)

In your application code, verify you are inside a transaction, then call `emit!`. This guarantees that the event is only persisted if the transaction commits.

```clojure
(require '[idem.outbox :as outbox]
         '[next.jdbc :as jdbc])

(defn create-order! [ds order-data]
  (jdbc/with-transaction [tx ds]
    ;; 1. Business Logic: Write to domain tables
    (sql/insert! tx :orders order-data)
    
    ;; 2. Outbox: Emit event in the SAME transaction
    ;; Uses the default PostgreSQL store implementation
    (outbox/emit! tx {:aggregate-type "order"
                      :aggregate-id   (:id order-data)
                      :event-type     "order.created"
                      :payload        order-data
                      :headers        {:trace-id "abc-123"}})))
```

### 3. Dispatcher (Relay)

The Dispatcher runs in the background. It polls the outbox table and pushes events to your message bus (Kafka, RabbitMQ, HTTP, etc.).

You must implement the `Publisher` protocol or provide a simple function adapter.

```clojure
(require '[idem.dispatcher :as dispatcher]
         '[idem.protocol :as protocol])

;; Option A: Simple Function Adapter (for simple use cases)
(def my-publisher 
  (protocol/fn->publisher 
    (fn [event] 
      (println "Publishing to Kafka:" (:event_id event)))))

;; Option B: Full Protocol (for connection management)
(defrecord KafkaPublisher [producer]
  protocol/Publisher
  (start! [this] (connect-kafka! ...))
  (stop!  [this] (close-kafka! ...))
  (publish! [this event] (kafka-send! producer event)))

;; Start the Dispatcher (uses default Postgres store)
(def stop-dispatcher! 
  (dispatcher/start! ds my-publisher {:poll-interval-ms 1000
                                      :batch-size 50
                                      :max-attempts 10}))

;; Stop it when app shuts down
(stop-dispatcher!)
```

### 4. Consumer (Inbox)

Wrap your message handling logic with `with-idempotency`. This ensures that even if the message bus delivers the same message twice, your handler runs only once.

```clojure
(require '[idem.inbox :as inbox])

(defn handle-message! [ds message]
  (let [consumer-group "order-service-group"
        message-id     (:id message)]
    
    ;; 'ds' is passed to the default PostgresInboxStore.
    ;; To use Redis, you would swap the implementation here.
    (inbox/with-idempotency ds consumer-group message-id {:ttl-ms 300000}
      (fn []
        ;; Your idempotent business logic here
        (println "Processing order:" (:payload message))))))
```

## Architecture & Extensibility

IDEM uses a **Store Protocol** pattern (`idem.protocol`) to separate logic from storage.

*   **Default**: `idem.impl.postgres` (included) - Uses PostgreSQL for Outbox (Transactional) and Inbox (Unique Constraints).
*   **Custom**: You can implement `OutboxStore` or `InboxStore` protocols to support other backends (e.g., **Redis** for high-throughput Inbox, or **MySQL**).

The public functions (`outbox/emit!`, `inbox/with-idempotency`) act as facades that delegate to these protocols, ensuring your business code remains unchanged even if you switch backends.

## Running Tests

To run the integration tests:

```bash
clojure -M:test
```

## License

MIT