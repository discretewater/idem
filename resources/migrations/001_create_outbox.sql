-- Migration: Create Outbox Table
-- Description: Stores events to be published reliably.

CREATE TABLE IF NOT EXISTS idem_outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB,
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed', 'dead')),
    attempts INT DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    last_error TEXT,
    dead_at TIMESTAMPTZ
);

-- Index for Dispatcher polling (performance critical)
CREATE INDEX IF NOT EXISTS idx_idem_outbox_pending 
ON idem_outbox_events (status, next_attempt_at, created_at);

-- Index for aggregate lookup (domain queries)
CREATE INDEX IF NOT EXISTS idx_idem_outbox_aggregate 
ON idem_outbox_events (aggregate_type, aggregate_id);
