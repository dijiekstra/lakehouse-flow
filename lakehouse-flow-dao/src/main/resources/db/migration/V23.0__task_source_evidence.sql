-- Keep task snapshot conclusions and source-health evidence independently queryable.
ALTER TABLE task_instance
    ADD COLUMN source_health VARCHAR(32),
    ADD COLUMN source_health_detail TEXT,
    ADD COLUMN source_evidence_checked_at TIMESTAMP;

ALTER TABLE task_instance
    ADD CONSTRAINT ck_task_source_health
    CHECK (source_health IS NULL OR source_health IN ('HEALTHY', 'REPAIRABLE', 'SOURCE_BLOCKED'));

CREATE INDEX idx_task_source_health
    ON task_instance(source_health, updated_at DESC);
