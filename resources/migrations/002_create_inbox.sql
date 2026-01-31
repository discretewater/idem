-- Migration: Create Inbox Table
-- Description: Stores received messages for idempotency and crash recovery.

CREATE TABLE IF NOT EXISTS idem_inbox_messages (
    consumer TEXT NOT NULL,
    message_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('processing', 'processed', 'failed')),
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    last_error TEXT,
    retry_count INT DEFAULT 0,
    PRIMARY KEY (consumer, message_id)
);

-- Index for finding stuck messages (recovery)
CREATE INDEX IF NOT EXISTS idx_idem_inbox_processing 
ON idem_inbox_messages (consumer, status, locked_until);
