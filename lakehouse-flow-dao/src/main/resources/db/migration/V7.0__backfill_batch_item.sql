-- V7.0__backfill_batch_item.sql
-- Persistent backfill batch and item records for scheduler-side replay and recovery.

CREATE TABLE IF NOT EXISTS backfill_batch (
    id BIGSERIAL PRIMARY KEY,
    batch_key VARCHAR(255) NOT NULL UNIQUE,
    action_key VARCHAR(255) NOT NULL,
    flow_plan_version_id BIGINT NOT NULL REFERENCES flow_plan_version(id),
    workflow_code VARCHAR(255) NOT NULL,
    workflow_version INTEGER NOT NULL,
    start_schedule_node_id BIGINT NOT NULL REFERENCES schedule_node(id),
    start_node_code VARCHAR(255) NOT NULL,
    biz_date_start DATE NOT NULL,
    biz_date_end DATE NOT NULL,
    cascade_policy VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    produced_workflow_count INTEGER NOT NULL DEFAULT 0,
    total_item_count INTEGER NOT NULL DEFAULT 0,
    requested_by VARCHAR(255),
    reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_backfill_batch_action_key
    ON backfill_batch(action_key);

CREATE INDEX IF NOT EXISTS idx_backfill_batch_flow_plan_version
    ON backfill_batch(flow_plan_version_id);

CREATE INDEX IF NOT EXISTS idx_backfill_batch_status
    ON backfill_batch(status, created_at DESC);

CREATE TABLE IF NOT EXISTS backfill_item (
    id BIGSERIAL PRIMARY KEY,
    backfill_batch_id BIGINT NOT NULL REFERENCES backfill_batch(id) ON DELETE CASCADE,
    biz_date DATE NOT NULL,
    flow_plan_version_id BIGINT NOT NULL REFERENCES flow_plan_version(id),
    schedule_node_id BIGINT NOT NULL REFERENCES schedule_node(id),
    node_code VARCHAR(255) NOT NULL,
    target_asset_key VARCHAR(255),
    workflow_instance_id BIGINT NOT NULL REFERENCES workflow_instance(id),
    task_instance_id BIGINT NOT NULL REFERENCES task_instance(id),
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_backfill_item_batch_node_date UNIQUE (backfill_batch_id, biz_date, node_code),
    CONSTRAINT uk_backfill_item_task_instance UNIQUE (task_instance_id)
);

CREATE INDEX IF NOT EXISTS idx_backfill_item_batch
    ON backfill_item(backfill_batch_id, biz_date ASC);

CREATE INDEX IF NOT EXISTS idx_backfill_item_flow_plan_version
    ON backfill_item(flow_plan_version_id);

CREATE INDEX IF NOT EXISTS idx_backfill_item_schedule_node
    ON backfill_item(schedule_node_id);

CREATE INDEX IF NOT EXISTS idx_backfill_item_status
    ON backfill_item(status, created_at ASC);
