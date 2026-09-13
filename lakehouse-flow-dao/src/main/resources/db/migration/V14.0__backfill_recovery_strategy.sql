-- V14.0__backfill_recovery_strategy.sql
-- Record whether a replacement replays the full scope or a failed-node branch.

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS recovery_strategy VARCHAR(64);

UPDATE backfill_batch
SET recovery_strategy = 'FULL_SCOPE'
WHERE source_backfill_batch_id IS NOT NULL
  AND recovery_strategy IS NULL;

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_recovery_strategy
        CHECK (
            (source_backfill_batch_id IS NULL AND recovery_strategy IS NULL)
            OR (source_backfill_batch_id IS NOT NULL
                AND recovery_strategy IN ('FULL_SCOPE', 'FAILED_NODE_CASCADE'))
        );
