-- ============================================================================
-- Lakehouse Flow - Core Database Schema (V1.0)
-- ============================================================================
-- This migration initializes the core tables for the event-driven lakehouse
-- scheduling system. It includes event storage, asset state, dependencies,
-- workflow/task instances, and audit tables.
-- ============================================================================

-- ============================================================================
-- 1. LAKEHOUSE_EVENT - Raw events from table formats (Paimon, Iceberg, Hudi)
-- ============================================================================
CREATE TABLE IF NOT EXISTS lakehouse_event (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(50) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    partition_name VARCHAR(255),
    snapshot_id VARCHAR(255),
    schema_id VARCHAR(255),
    watermark TIMESTAMP,
    commit_kind VARCHAR(50),
    commit_time TIMESTAMP,
    payload_json JSONB,
    observed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_lakehouse_event_event_id ON lakehouse_event(event_id);
CREATE INDEX idx_lakehouse_event_asset_snapshot ON lakehouse_event(catalog_name, database_name, table_name, partition_name, snapshot_id);
CREATE INDEX idx_lakehouse_event_source_time ON lakehouse_event(source_type, observed_at DESC);
CREATE INDEX idx_lakehouse_event_created_at ON lakehouse_event(created_at DESC);

-- ============================================================================
-- 2. ASSET_STATE - Current scheduling truth of data assets
-- ============================================================================
CREATE TABLE IF NOT EXISTS asset_state (
    id BIGSERIAL PRIMARY KEY,
    asset_key VARCHAR(255) NOT NULL UNIQUE,
    asset_type VARCHAR(50) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    partition_name VARCHAR(255),
    latest_snapshot_id VARCHAR(255),
    latest_schema_id VARCHAR(255),
    latest_watermark TIMESTAMP,
    latest_commit_time TIMESTAMP,
    quality_status VARCHAR(50) DEFAULT 'UNKNOWN',
    schema_status VARCHAR(50) DEFAULT 'UNKNOWN',
    backfill_status VARCHAR(50) DEFAULT 'NONE',
    readiness_status VARCHAR(50) DEFAULT 'UNKNOWN',
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_asset_state_asset_key ON asset_state(asset_key);
CREATE INDEX idx_asset_state_readiness ON asset_state(readiness_status, updated_at DESC);
CREATE INDEX idx_asset_state_catalog_table ON asset_state(catalog_name, database_name, table_name);

-- ============================================================================
-- 3. ASSET_DEPENDENCY - What workflows/tasks are waiting for
-- ============================================================================
CREATE TABLE IF NOT EXISTS asset_dependency (
    id BIGSERIAL PRIMARY KEY,
    asset_key VARCHAR(255) NOT NULL,
    workflow_code VARCHAR(255),
    task_code VARCHAR(255),
    dependency_conditions JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_asset_dependency_asset_key ON asset_dependency(asset_key, enabled);
CREATE INDEX idx_asset_dependency_workflow ON asset_dependency(workflow_code, enabled);
CREATE INDEX idx_asset_dependency_task ON asset_dependency(task_code, enabled);

-- ============================================================================
-- 4. WORKFLOW_DEFINITION - Reusable DAG definitions
-- ============================================================================
CREATE TABLE IF NOT EXISTS workflow_definition (
    id BIGSERIAL PRIMARY KEY,
    code VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    owner VARCHAR(255),
    definition_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, version)
);

CREATE INDEX idx_workflow_definition_code ON workflow_definition(code);
CREATE INDEX idx_workflow_definition_status ON workflow_definition(status);

-- ============================================================================
-- 5. TASK_DEFINITION - Tasks within workflows
-- ============================================================================
CREATE TABLE IF NOT EXISTS task_definition (
    id BIGSERIAL PRIMARY KEY,
    code VARCHAR(255) NOT NULL,
    workflow_code VARCHAR(255) NOT NULL,
    workflow_version INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    task_type VARCHAR(50) NOT NULL,
    task_params_json JSONB,
    upstream_task_codes VARCHAR(1024),
    asset_dependencies_json JSONB,
    timeout_seconds INTEGER DEFAULT 86400,
    retry_policy_json JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_code, workflow_version) REFERENCES workflow_definition(code, version)
);

CREATE INDEX idx_task_definition_code ON task_definition(code);
CREATE INDEX idx_task_definition_workflow ON task_definition(workflow_code, workflow_version);

-- ============================================================================
-- 6. WORKFLOW_INSTANCE - One execution of a workflow
-- ============================================================================
CREATE TABLE IF NOT EXISTS workflow_instance (
    id BIGSERIAL PRIMARY KEY,
    instance_key VARCHAR(255) NOT NULL UNIQUE,
    workflow_code VARCHAR(255) NOT NULL,
    workflow_version INTEGER NOT NULL,
    biz_date DATE NOT NULL,
    trigger_type VARCHAR(50) NOT NULL,
    trigger_event_id VARCHAR(255),
    trigger_reason TEXT,
    state VARCHAR(50) NOT NULL DEFAULT 'CREATED',
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_workflow_instance_instance_key ON workflow_instance(instance_key);
CREATE INDEX idx_workflow_instance_workflow ON workflow_instance(workflow_code, workflow_version);
CREATE INDEX idx_workflow_instance_state ON workflow_instance(state, updated_at DESC);
CREATE INDEX idx_workflow_instance_biz_date ON workflow_instance(biz_date DESC);

-- ============================================================================
-- 7. TASK_INSTANCE - One execution of a task
-- ============================================================================
CREATE TABLE IF NOT EXISTS task_instance (
    id BIGSERIAL PRIMARY KEY,
    instance_key VARCHAR(255) NOT NULL UNIQUE,
    workflow_instance_id BIGINT NOT NULL,
    task_code VARCHAR(255) NOT NULL,
    task_version INTEGER NOT NULL,
    biz_date DATE NOT NULL,
    state VARCHAR(50) NOT NULL DEFAULT 'CREATED',
    waiting_reason TEXT,
    try_number INTEGER NOT NULL DEFAULT 1,
    max_retries INTEGER NOT NULL DEFAULT 3,
    executor_type VARCHAR(50),
    external_job_id VARCHAR(255),
    submit_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_instance_id) REFERENCES workflow_instance(id) ON DELETE CASCADE
);

CREATE INDEX idx_task_instance_instance_key ON task_instance(instance_key);
CREATE INDEX idx_task_instance_workflow_instance ON task_instance(workflow_instance_id, task_code);
CREATE INDEX idx_task_instance_state ON task_instance(state, updated_at DESC);
CREATE INDEX idx_task_instance_external_job ON task_instance(external_job_id) WHERE external_job_id IS NOT NULL;

-- ============================================================================
-- 8. TRIGGER_HISTORY - Audit of why instances were created/released
-- ============================================================================
CREATE TABLE IF NOT EXISTS trigger_history (
    id BIGSERIAL PRIMARY KEY,
    trigger_key VARCHAR(255) NOT NULL UNIQUE,
    trigger_type VARCHAR(50) NOT NULL,
    asset_key VARCHAR(255),
    snapshot_id VARCHAR(255),
    watermark TIMESTAMP,
    event_id VARCHAR(255),
    workflow_instance_id BIGINT,
    task_instance_id BIGINT,
    decision VARCHAR(50) NOT NULL,
    decision_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_instance_id) REFERENCES workflow_instance(id) ON DELETE CASCADE,
    FOREIGN KEY (task_instance_id) REFERENCES task_instance(id) ON DELETE CASCADE
);

CREATE INDEX idx_trigger_history_trigger_key ON trigger_history(trigger_key);
CREATE INDEX idx_trigger_history_asset_key ON trigger_history(asset_key);
CREATE INDEX idx_trigger_history_workflow_instance ON trigger_history(workflow_instance_id);

-- ============================================================================
-- 9. EXECUTOR_JOB - External job tracking (Spark, Flink, Shell, HTTP, etc.)
-- ============================================================================
CREATE TABLE IF NOT EXISTS executor_job (
    id BIGSERIAL PRIMARY KEY,
    external_job_id VARCHAR(255) NOT NULL UNIQUE,
    task_instance_id BIGINT NOT NULL,
    executor_type VARCHAR(50) NOT NULL,
    job_status VARCHAR(50) NOT NULL DEFAULT 'SUBMITTED',
    submit_time TIMESTAMP NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    job_url TEXT,
    error_message TEXT,
    output_json JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_instance_id) REFERENCES task_instance(id) ON DELETE CASCADE
);

CREATE INDEX idx_executor_job_external_job_id ON executor_job(external_job_id);
CREATE INDEX idx_executor_job_task_instance ON executor_job(task_instance_id);
CREATE INDEX idx_executor_job_status ON executor_job(job_status, updated_at DESC);

-- ============================================================================
-- 10. SCHEDULER_LEASE - Distributed coordination for multi-node schedulers
-- ============================================================================
CREATE TABLE IF NOT EXISTS scheduler_lease (
    id BIGSERIAL PRIMARY KEY,
    lease_name VARCHAR(255) NOT NULL UNIQUE,
    owner_id VARCHAR(255),
    fencing_token BIGINT,
    expire_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_scheduler_lease_lease_name ON scheduler_lease(lease_name);
CREATE INDEX idx_scheduler_lease_expire_at ON scheduler_lease(expire_at DESC);

-- ============================================================================
-- 11. EVENT_CONSUMER_OFFSET - Track event ingestion progress per source
-- ============================================================================
CREATE TABLE IF NOT EXISTS event_consumer_offset (
    id BIGSERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255),
    last_processed_offset VARCHAR(255),
    last_processed_time TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_type, source_name, consumer_group)
);

CREATE INDEX idx_event_consumer_offset_source ON event_consumer_offset(source_type, source_name);

-- ============================================================================
-- 12. BACKFILL_SPEC - Configuration for backfill operations
-- ============================================================================
CREATE TABLE IF NOT EXISTS backfill_spec (
    id BIGSERIAL PRIMARY KEY,
    code VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'CREATED',
    workflow_code VARCHAR(255) NOT NULL,
    workflow_version INTEGER NOT NULL,
    start_biz_date DATE NOT NULL,
    end_biz_date DATE NOT NULL,
    asset_scope VARCHAR(1024),
    trigger_strategy VARCHAR(50) NOT NULL DEFAULT 'ONCE',
    priority INTEGER NOT NULL DEFAULT 0,
    overwrite_output BOOLEAN NOT NULL DEFAULT false,
    created_by VARCHAR(255),
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_backfill_spec_code ON backfill_spec(code);
CREATE INDEX idx_backfill_spec_status ON backfill_spec(status);
CREATE INDEX idx_backfill_spec_workflow ON backfill_spec(workflow_code, workflow_version);

-- ============================================================================
-- Summary of core tables:
-- ============================================================================
-- lakehouse_event: Raw events from table formats
-- asset_state: Current asset readiness status
-- asset_dependency: Workflow/task dependencies
-- workflow_definition: Reusable workflow DAGs (versioned)
-- task_definition: Task definitions within workflows
-- workflow_instance: Workflow execution instances
-- task_instance: Task execution instances
-- trigger_history: Audit trail of trigger decisions
-- executor_job: External job tracking
-- scheduler_lease: Distributed coordination
-- event_consumer_offset: Event ingestion offsets
-- backfill_spec: Backfill operation specifications
-- ============================================================================
