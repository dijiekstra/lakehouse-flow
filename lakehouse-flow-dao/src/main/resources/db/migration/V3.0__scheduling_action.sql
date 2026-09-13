-- V3.0__scheduling_action.sql
-- Durable audit and idempotency table for scheduling-side actions.

CREATE TABLE IF NOT EXISTS scheduling_action (
    id BIGSERIAL PRIMARY KEY,
    action_key VARCHAR(255) NOT NULL UNIQUE,
    action_type VARCHAR(64) NOT NULL,
    scope_type VARCHAR(64) NOT NULL,
    workflow_code VARCHAR(255),
    workflow_version INTEGER,
    workflow_instance_id BIGINT REFERENCES workflow_instance(id),
    task_instance_id BIGINT REFERENCES task_instance(id),
    biz_date_start DATE,
    biz_date_end DATE,
    target_asset_key VARCHAR(255),
    target_snapshot_id VARCHAR(255),
    requested_by VARCHAR(255),
    reason TEXT,
    status VARCHAR(32) NOT NULL DEFAULT 'ACCEPTED',
    result_message TEXT,
    produced_workflow_instance_id BIGINT REFERENCES workflow_instance(id),
    produced_task_instance_id BIGINT REFERENCES task_instance(id),
    request_payload_json JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduling_action_key
    ON scheduling_action(action_key);

CREATE INDEX IF NOT EXISTS idx_scheduling_action_type
    ON scheduling_action(action_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_scheduling_action_status
    ON scheduling_action(status, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_scheduling_action_workflow_instance
    ON scheduling_action(workflow_instance_id);

CREATE INDEX IF NOT EXISTS idx_scheduling_action_task_instance
    ON scheduling_action(task_instance_id);
