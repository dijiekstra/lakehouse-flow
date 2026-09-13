-- Preserve immutable lineage when a failed backfill is recovered by a replacement batch.

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS source_backfill_batch_id BIGINT;

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS recovery_attempt INTEGER NOT NULL DEFAULT 0;

ALTER TABLE backfill_batch
    ADD CONSTRAINT fk_backfill_batch_source
        FOREIGN KEY (source_backfill_batch_id) REFERENCES backfill_batch(id);

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_recovery_attempt
        CHECK (recovery_attempt >= 0);

CREATE UNIQUE INDEX IF NOT EXISTS uk_backfill_batch_source
    ON backfill_batch(source_backfill_batch_id);
