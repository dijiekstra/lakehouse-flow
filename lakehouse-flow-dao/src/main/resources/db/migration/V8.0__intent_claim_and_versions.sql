-- V8.0__intent_claim_and_versions.sql
-- Protect scheduler state changes and make downstream intent delivery idempotent.

ALTER TABLE task_instance
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE workflow_instance
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE task_instance
    ADD COLUMN IF NOT EXISTS delivered_to VARCHAR(255);

ALTER TABLE task_instance
    ADD COLUMN IF NOT EXISTS delivery_key VARCHAR(255);

CREATE UNIQUE INDEX IF NOT EXISTS uk_task_intent_delivery
    ON task_instance(delivered_to, delivery_key)
    WHERE delivered_to IS NOT NULL AND delivery_key IS NOT NULL;
