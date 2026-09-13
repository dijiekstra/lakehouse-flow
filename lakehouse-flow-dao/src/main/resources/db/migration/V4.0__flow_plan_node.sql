-- V4.0__flow_plan_node.sql
-- FlowPlan, version, and node definitions for snapshot-driven scheduling.

CREATE TABLE IF NOT EXISTS flow_plan (
    id BIGSERIAL PRIMARY KEY,
    flow_code VARCHAR(255) NOT NULL UNIQUE,
    flow_name VARCHAR(255) NOT NULL,
    flow_space_code VARCHAR(255),
    owner VARCHAR(255),
    description TEXT,
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT',
    current_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_flow_plan_code
    ON flow_plan(flow_code);

CREATE INDEX IF NOT EXISTS idx_flow_plan_status
    ON flow_plan(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_flow_plan_space
    ON flow_plan(flow_space_code, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_flow_plan_owner
    ON flow_plan(owner, updated_at DESC);

CREATE TABLE IF NOT EXISTS flow_plan_version (
    id BIGSERIAL PRIMARY KEY,
    flow_plan_id BIGINT NOT NULL REFERENCES flow_plan(id) ON DELETE CASCADE,
    flow_code VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT',
    graph_json JSONB,
    dependency_spec_json JSONB,
    trigger_policy_json JSONB,
    confirmation_policy_json JSONB,
    concurrency_policy_json JSONB,
    published_by VARCHAR(255),
    published_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_flow_plan_version UNIQUE (flow_plan_id, version)
);

CREATE INDEX IF NOT EXISTS idx_flow_plan_version_plan
    ON flow_plan_version(flow_plan_id, version DESC);

CREATE INDEX IF NOT EXISTS idx_flow_plan_version_code
    ON flow_plan_version(flow_code, version DESC);

CREATE INDEX IF NOT EXISTS idx_flow_plan_version_status
    ON flow_plan_version(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS schedule_node (
    id BIGSERIAL PRIMARY KEY,
    flow_plan_version_id BIGINT NOT NULL REFERENCES flow_plan_version(id) ON DELETE CASCADE,
    node_code VARCHAR(255) NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    node_type VARCHAR(64) NOT NULL,
    depends_on_nodes_json JSONB,
    input_dependency_spec_json JSONB,
    output_asset_key VARCHAR(255),
    confirmation_policy_json JSONB,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_schedule_node_version_code UNIQUE (flow_plan_version_id, node_code)
);

CREATE INDEX IF NOT EXISTS idx_schedule_node_version
    ON schedule_node(flow_plan_version_id, sort_order ASC);

CREATE INDEX IF NOT EXISTS idx_schedule_node_code
    ON schedule_node(node_code);

CREATE INDEX IF NOT EXISTS idx_schedule_node_output_asset
    ON schedule_node(output_asset_key)
    WHERE output_asset_key IS NOT NULL;
