-- Persist explicit whole-date backfill skip policy and its snapshot-confirmation evidence.

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS skip_policy VARCHAR(64) NOT NULL DEFAULT 'NONE';

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS skipped_date_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS skip_evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_skip_policy
        CHECK (skip_policy IN ('NONE', 'SKIP_FULLY_CONFIRMED_DATES'));

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_skipped_date_count
        CHECK (skipped_date_count >= 0);
