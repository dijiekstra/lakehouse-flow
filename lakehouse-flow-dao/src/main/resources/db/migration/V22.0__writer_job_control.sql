-- G20: global single-writer ownership and independent platform job-control intents.

CREATE TABLE writer_job_binding (
    id BIGSERIAL PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0,
    writer_job_key VARCHAR(255) NOT NULL,
    table_asset_key VARCHAR(255) NOT NULL,
    allowed_processing_modes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    current_writer_epoch BIGINT NOT NULL DEFAULT 0,
    active_processing_mode VARCHAR(32),
    holder_intent_key VARCHAR(255),
    holder_expires_at TIMESTAMP,
    current_control_intent_key VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_writer_job_binding_key UNIQUE (writer_job_key),
    CONSTRAINT uk_writer_job_binding_table UNIQUE (table_asset_key),
    CONSTRAINT ck_writer_binding_epoch CHECK (current_writer_epoch >= 0)
);

CREATE INDEX idx_writer_binding_table ON writer_job_binding(table_asset_key);

ALTER TABLE schedule_node
    ADD COLUMN writer_job_key VARCHAR(255);

CREATE INDEX idx_schedule_node_writer_job ON schedule_node(writer_job_key);

ALTER TABLE scheduling_intent
    ADD COLUMN writer_job_key VARCHAR(255),
    ADD COLUMN writer_epoch BIGINT;

ALTER TABLE scheduling_intent
    ADD CONSTRAINT ck_scheduling_intent_writer_pair
        CHECK ((writer_job_key IS NULL AND writer_epoch IS NULL)
            OR (writer_job_key IS NOT NULL AND writer_epoch IS NOT NULL AND writer_epoch > 0));

CREATE INDEX idx_scheduling_intent_writer_epoch
    ON scheduling_intent(writer_job_key, writer_epoch, created_at);

CREATE TABLE job_control_intent (
    id BIGSERIAL PRIMARY KEY,
    contract_version VARCHAR(32) NOT NULL,
    request_key VARCHAR(255) NOT NULL,
    intent_key VARCHAR(255) NOT NULL,
    writer_job_binding_id BIGINT NOT NULL REFERENCES writer_job_binding(id),
    writer_job_key VARCHAR(255) NOT NULL,
    table_asset_key VARCHAR(255) NOT NULL,
    operation_type VARCHAR(32) NOT NULL,
    processing_mode VARCHAR(32) NOT NULL,
    writer_epoch BIGINT NOT NULL,
    previous_writer_epoch BIGINT NOT NULL,
    baseline_snapshot_id VARCHAR(255),
    deliver_before TIMESTAMP NOT NULL,
    confirmation_deadline TIMESTAMP NOT NULL,
    requested_by VARCHAR(255) NOT NULL,
    reason TEXT,
    snapshot_result VARCHAR(32) NOT NULL DEFAULT 'WAITING',
    observed_snapshot_id VARCHAR(255),
    waiting_reason TEXT,
    source_health VARCHAR(32),
    source_health_detail TEXT,
    source_evidence_checked_at TIMESTAMP,
    last_snapshot_check_at TIMESTAMP,
    instruction_payload_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_job_control_intent_key UNIQUE (intent_key),
    CONSTRAINT uk_job_control_request_key UNIQUE (request_key),
    CONSTRAINT uk_job_control_writer_epoch UNIQUE (writer_job_binding_id, writer_epoch),
    CONSTRAINT ck_job_control_epoch CHECK (writer_epoch > 0 AND previous_writer_epoch >= 0),
    CONSTRAINT ck_job_control_snapshot_result CHECK (
        snapshot_result IN ('WAITING', 'SNAPSHOT_CONFIRMED', 'SNAPSHOT_NOT_ADVANCED'))
);

CREATE INDEX idx_job_control_writer
    ON job_control_intent(writer_job_key, created_at DESC);
CREATE INDEX idx_job_control_snapshot
    ON job_control_intent(snapshot_result, confirmation_deadline ASC);

CREATE TABLE job_control_intent_delivery (
    id BIGSERIAL PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0,
    job_control_intent_id BIGINT NOT NULL REFERENCES job_control_intent(id),
    channel VARCHAR(64) NOT NULL,
    destination VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    claim_owner VARCHAR(255),
    claim_token VARCHAR(64),
    claim_expires_at TIMESTAMP,
    deliver_before TIMESTAMP NOT NULL,
    published_at TIMESTAMP,
    dead_lettered_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_job_control_delivery_intent UNIQUE (job_control_intent_id),
    CONSTRAINT ck_job_control_delivery_status CHECK (
        status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'RETRY_WAIT', 'EXHAUSTED'))
);

CREATE INDEX idx_job_control_delivery_claim
    ON job_control_intent_delivery(status, next_attempt_at, claim_expires_at, created_at);
