-- V15.0__task_intent_claim_lease.sql
-- Reserve ready scheduling intents without turning scheduler state into execution state.
-- Superseded by V16, which moves all transport concerns out of task_instance.

ALTER TABLE task_instance
    ADD COLUMN IF NOT EXISTS claim_owner VARCHAR(255);

ALTER TABLE task_instance
    ADD COLUMN IF NOT EXISTS claim_key VARCHAR(255);

ALTER TABLE task_instance
    ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMP;

ALTER TABLE task_instance
    ADD CONSTRAINT ck_task_intent_claim_lease
        CHECK (
            (claim_owner IS NULL AND claim_key IS NULL AND claim_expires_at IS NULL)
            OR (claim_owner IS NOT NULL AND claim_key IS NOT NULL AND claim_expires_at IS NOT NULL)
        );

CREATE INDEX IF NOT EXISTS idx_task_claim_owner_key
    ON task_instance(claim_owner, claim_key)
    WHERE claim_owner IS NOT NULL AND claim_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_task_claim_expiry
    ON task_instance(state, claim_expires_at)
    WHERE state = 'READY_TO_SCHEDULE';
