-- Persist source reconciliation evidence used to gate snapshot timeout decisions.
CREATE TABLE snapshot_source_health (
    id BIGSERIAL PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0,
    source_type VARCHAR(50) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    table_asset_key VARCHAR(255) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    offset_status VARCHAR(64) NOT NULL,
    projection_status VARCHAR(64) NOT NULL,
    durable_offset VARCHAR(255),
    latest_source_offset VARCHAR(255),
    evidence_checked_at TIMESTAMP NOT NULL,
    detail TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_snapshot_source_health_source UNIQUE (source_type, source_name),
    CONSTRAINT uk_snapshot_source_health_asset UNIQUE (table_asset_key)
);

CREATE INDEX idx_snapshot_source_health_outcome
    ON snapshot_source_health(outcome, evidence_checked_at DESC);

-- Existing published definitions are bounded batch nodes unless explicitly migrated later.
ALTER TABLE schedule_node
    ADD COLUMN processing_mode VARCHAR(32) NOT NULL DEFAULT 'BATCH';

ALTER TABLE schedule_node
    ADD CONSTRAINT ck_schedule_node_processing_mode
    CHECK (processing_mode IN ('STREAMING', 'BATCH'));

-- Explicit action entry points retain their documented parent-dependency bypass at publication time.
ALTER TABLE task_instance
    ADD COLUMN parent_dependency_bypassed BOOLEAN NOT NULL DEFAULT false;

-- SchedulingIntent 1.3 freezes the engine-neutral mode and complete input evidence vector.
ALTER TABLE scheduling_intent
    ADD COLUMN processing_mode VARCHAR(32) NOT NULL DEFAULT 'BATCH';

ALTER TABLE scheduling_intent
    ADD COLUMN input_snapshot_vector_json JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE scheduling_intent
    ADD CONSTRAINT ck_scheduling_intent_processing_mode
    CHECK (processing_mode IN ('STREAMING', 'BATCH'));
