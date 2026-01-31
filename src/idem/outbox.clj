(ns idem.outbox
  "Outbox Core Module: Responsible for writing events within a business transaction.
   
   This namespace acts as a facade over the `OutboxStore` protocol.
   By default, it uses the PostgreSQL implementation."
  (:require [idem.protocol :as protocol]
            [idem.impl.postgres :as pg]))

(def ^:dynamic *store* 
  "The default OutboxStore implementation.
   Can be rebound for testing or alternative storage backends."
  (pg/->outbox-store {}))

(defn emit!
  "Records an event in the current database transaction (Outbox Pattern).
   
   Ensures atomicity between business state changes and event publishing.
   
   Parameters:
     tx        - The active database transaction (e.g., next.jdbc/Connection).
     event-map - Event details map containing:
       :aggregate-type (String)
       :aggregate-id   (String/UUID)
       :event-type     (String)
       :payload        (Map)
       :headers        (Map, optional)

   Returns:
     A map containing the generated {:event-id ...}.

   Example:
     (jdbc/with-transaction [tx ds]
       (create-order! tx ...)
       (outbox/emit! tx {:aggregate-type 'order'
                         :aggregate-id   order-id
                         :event-type     'order.created'
                         :payload        {...}}))"
  [tx event-map]
  (protocol/emit! *store* tx event-map))