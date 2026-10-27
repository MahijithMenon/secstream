-- One row per (logical) security event. event_id is the pipeline's content/upstream-derived id,
-- so retried or replayed inserts are idempotent via ON CONFLICT DO NOTHING.
CREATE TABLE security_events (
    event_id          CHAR(32)     PRIMARY KEY,
    source_event_id   TEXT,
    source            TEXT         NOT NULL,
    format            TEXT         NOT NULL,
    category          TEXT         NOT NULL,
    outcome           TEXT         NOT NULL,
    severity          TEXT         NOT NULL,
    occurred_at       TIMESTAMPTZ  NOT NULL,
    received_at       TIMESTAMPTZ  NOT NULL,
    user_id           TEXT,
    device_id         TEXT,
    application       TEXT,
    source_ip         INET,
    destination_ip    INET,
    destination_port  INTEGER      CHECK (destination_port BETWEEN 0 AND 65535),
    attributes        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    inserted_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX security_events_occurred_at_idx ON security_events (occurred_at);
CREATE INDEX security_events_user_idx ON security_events (user_id, occurred_at) WHERE user_id IS NOT NULL;
