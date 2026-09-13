-- V17.0__scheduling_intent_contract.sql
-- Persist one complete channel-neutral instruction and its snapshot attribution context.

ALTER TABLE scheduling_intent
    ADD COLUMN contract_version VARCHAR(32) NOT NULL DEFAULT '0.legacy',
    ADD COLUMN trigger_type VARCHAR(50),
    ADD COLUMN backfill_batch_id BIGINT,
    ADD COLUMN backfill_item_id BIGINT,
    ADD COLUMN instruction_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE scheduling_intent intent
SET trigger_type = workflow.trigger_type
FROM workflow_instance workflow
WHERE workflow.id = intent.workflow_instance_id
  AND intent.trigger_type IS NULL;

ALTER TABLE scheduling_intent
    ALTER COLUMN trigger_type SET NOT NULL;

CREATE INDEX idx_scheduling_intent_backfill_batch
    ON scheduling_intent(backfill_batch_id, created_at ASC)
    WHERE backfill_batch_id IS NOT NULL;

CREATE UNIQUE INDEX uk_scheduling_intent_backfill_item
    ON scheduling_intent(backfill_item_id)
    WHERE backfill_item_id IS NOT NULL;
