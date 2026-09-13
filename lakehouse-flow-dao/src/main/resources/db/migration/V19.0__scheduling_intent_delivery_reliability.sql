-- V19.0__scheduling_intent_delivery_reliability.sql
-- Add scheduler-owned publication leases, retry timing, and dead-letter evidence.

ALTER TABLE scheduling_intent_delivery
    ADD COLUMN next_attempt_at TIMESTAMP,
    ADD COLUMN claim_owner VARCHAR(255),
    ADD COLUMN claim_token VARCHAR(64),
    ADD COLUMN claim_expires_at TIMESTAMP,
    ADD COLUMN deliver_before TIMESTAMP,
    ADD COLUMN dead_lettered_at TIMESTAMP;

ALTER TABLE scheduling_intent_delivery
    DROP CONSTRAINT ck_scheduling_intent_delivery_status;

ALTER TABLE scheduling_intent_delivery
    ADD CONSTRAINT ck_scheduling_intent_delivery_status
        CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'RETRY_WAIT', 'EXHAUSTED')),
    ADD CONSTRAINT ck_scheduling_intent_delivery_claim
        CHECK (
            (status = 'PUBLISHING'
                AND claim_owner IS NOT NULL
                AND claim_token IS NOT NULL
                AND claim_expires_at IS NOT NULL)
            OR
            (status <> 'PUBLISHING'
                AND claim_owner IS NULL
                AND claim_token IS NULL
                AND claim_expires_at IS NULL)
        );

DROP INDEX IF EXISTS idx_intent_delivery_status;

CREATE INDEX idx_intent_delivery_claimable
    ON scheduling_intent_delivery(status, next_attempt_at ASC, claim_expires_at ASC, created_at ASC);
