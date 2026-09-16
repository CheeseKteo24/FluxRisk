CREATE TABLE IF NOT EXISTS risk_events (
    event_id text PRIMARY KEY,
    account_id text NOT NULL,
    device_id text NOT NULL,
    amount numeric(18, 2) NOT NULL CHECK (amount > 0),
    currency text NOT NULL,
    country text NOT NULL,
    occurred_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_risk_events_account_occurred
    ON risk_events (account_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS risk_decisions (
    event_id text PRIMARY KEY REFERENCES risk_events(event_id),
    account_id text NOT NULL,
    action text NOT NULL CHECK (action IN ('allow', 'review', 'block')),
    score integer NOT NULL CHECK (score BETWEEN 0 AND 100),
    features jsonb NOT NULL,
    rule_hits jsonb NOT NULL,
    decided_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_risk_decisions_account_decided
    ON risk_decisions (account_id, decided_at DESC);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id uuid PRIMARY KEY,
    aggregate_id text NOT NULL,
    message_type text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    published_at timestamptz NULL,
    attempts integer NOT NULL DEFAULT 0,
    last_error text NULL
);

CREATE INDEX IF NOT EXISTS ix_outbox_messages_pending
    ON outbox_messages (occurred_at)
    WHERE published_at IS NULL;
