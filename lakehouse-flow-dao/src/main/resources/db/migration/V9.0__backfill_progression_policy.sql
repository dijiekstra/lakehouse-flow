-- Add scheduler-side date progression controls to backfill batches.

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS progression_mode VARCHAR(64) NOT NULL DEFAULT 'PARALLEL';

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS max_active_dates INTEGER;

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_progression_mode
        CHECK (progression_mode IN ('PARALLEL', 'SERIAL', 'PARALLEL_WITH_LIMIT'));

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_progression_policy
        CHECK (
            (progression_mode = 'PARALLEL' AND max_active_dates IS NULL)
            OR (progression_mode = 'SERIAL' AND max_active_dates = 1)
            OR (progression_mode = 'PARALLEL_WITH_LIMIT' AND max_active_dates > 0)
        );
