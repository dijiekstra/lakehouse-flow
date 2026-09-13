-- V16.0__scheduling_intent_outbox.sql
-- Move transport concerns out of task_instance and create a durable intent outbox.

CREATE TABLE scheduling_intent (
    id BIGSERIAL PRIMARY KEY,
    intent_key VARCHAR(255) NOT NULL,
    task_instance_id BIGINT NOT NULL,
    workflow_instance_id BIGINT NOT NULL,
    flow_plan_version_id BIGINT,
    schedule_node_id BIGINT,
    task_code VARCHAR(255) NOT NULL,
    task_version INTEGER NOT NULL,
    biz_date TIMESTAMP NOT NULL,
    target_asset_key VARCHAR(255) NOT NULL,
    baseline_snapshot_id VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_scheduling_intent_key UNIQUE (intent_key),
    CONSTRAINT uk_scheduling_intent_task UNIQUE (task_instance_id),
    CONSTRAINT fk_scheduling_intent_task
        FOREIGN KEY (task_instance_id) REFERENCES task_instance(id) ON DELETE RESTRICT,
    CONSTRAINT fk_scheduling_intent_workflow
        FOREIGN KEY (workflow_instance_id) REFERENCES workflow_instance(id) ON DELETE RESTRICT
);

CREATE INDEX idx_scheduling_intent_workflow
    ON scheduling_intent(workflow_instance_id, created_at ASC);

CREATE INDEX idx_scheduling_intent_asset
    ON scheduling_intent(target_asset_key, created_at ASC);

CREATE TABLE scheduling_intent_delivery (
    id BIGSERIAL PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0,
    scheduling_intent_id BIGINT NOT NULL,
    channel VARCHAR(64) NOT NULL,
    destination VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TIMESTAMP,
    published_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_scheduling_intent_delivery_intent
        UNIQUE (scheduling_intent_id),
    CONSTRAINT fk_scheduling_intent_delivery_intent
        FOREIGN KEY (scheduling_intent_id) REFERENCES scheduling_intent(id) ON DELETE CASCADE,
    CONSTRAINT ck_scheduling_intent_delivery_status
        CHECK (status IN ('PENDING', 'PUBLISHED', 'RETRY_WAIT', 'EXHAUSTED')),
    CONSTRAINT ck_scheduling_intent_delivery_attempt_count
        CHECK (attempt_count >= 0)
);

CREATE INDEX idx_intent_delivery_status
    ON scheduling_intent_delivery(status, updated_at ASC);

CREATE INDEX idx_intent_delivery_intent
    ON scheduling_intent_delivery(scheduling_intent_id);

DROP INDEX IF EXISTS uk_task_intent_delivery;
DROP INDEX IF EXISTS idx_task_claim_owner_key;
DROP INDEX IF EXISTS idx_task_claim_expiry;

ALTER TABLE task_instance
    DROP CONSTRAINT IF EXISTS ck_task_intent_claim_lease;

ALTER TABLE task_instance
    DROP COLUMN IF EXISTS claim_owner,
    DROP COLUMN IF EXISTS claim_key,
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS delivered_to,
    DROP COLUMN IF EXISTS delivery_key;
