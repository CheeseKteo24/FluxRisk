ALTER TABLE risk_decisions
    ADD COLUMN IF NOT EXISTS event_time jsonb NULL,
    ADD COLUMN IF NOT EXISTS model_assessment jsonb NULL;

ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS locked_until timestamptz NULL,
    ADD COLUMN IF NOT EXISTS locked_by text NULL,
    ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS broker_metadata text NULL;

DROP INDEX IF EXISTS ix_outbox_messages_pending;
CREATE INDEX IF NOT EXISTS ix_outbox_messages_dispatch
    ON outbox_messages (available_at, occurred_at)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;

CREATE TABLE IF NOT EXISTS review_cases (
    id uuid PRIMARY KEY,
    event_id text NOT NULL UNIQUE REFERENCES risk_decisions(event_id),
    account_id text NOT NULL,
    suggested_action text NOT NULL CHECK (suggested_action IN ('review', 'block')),
    score integer NOT NULL CHECK (score BETWEEN 0 AND 100),
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'approved', 'rejected')),
    assignee text NULL,
    notes text NULL,
    version integer NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_review_cases_status_created
    ON review_cases (status, created_at DESC);
