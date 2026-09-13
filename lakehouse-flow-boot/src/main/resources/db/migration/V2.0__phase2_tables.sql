-- V2.0__phase2_tables.sql
-- Phase 2: Trigger History and Event Consumer Offset tables

-- TriggerHistory table
-- Audit log for workflow and task instance creation
CREATE TABLE IF NOT EXISTS trigger_history (
    id BIGSERIAL PRIMARY KEY,
    
    trigger_key VARCHAR(255) NOT NULL UNIQUE,
    trigger_type VARCHAR(32) NOT NULL,  -- SNAPSHOT_DRIVEN, SCHEDULED, MANUAL, BACKFILL
    
    asset_key VARCHAR(255),
    snapshot_id VARCHAR(255),
    watermark TIMESTAMP,
    event_id VARCHAR(255),
    
    workflow_instance_id BIGINT REFERENCES workflow_instance(id),
    task_instance_id BIGINT REFERENCES task_instance(id),
    
    decision VARCHAR(32) NOT NULL,  -- TRIGGERED, SKIPPED
    decision_reason TEXT,
    evaluation_payload_json JSONB,
    
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for trigger_history
CREATE INDEX idx_trigger_history_workflow ON trigger_history(workflow_instance_id);
CREATE INDEX idx_trigger_history_task ON trigger_history(task_instance_id);
CREATE INDEX idx_trigger_history_asset ON trigger_history(asset_key);
CREATE INDEX idx_trigger_history_created_at ON trigger_history(created_at DESC);
CREATE INDEX idx_trigger_history_type ON trigger_history(trigger_type);

-- EventConsumerOffset table
-- Tracks progress of event consumption from each source
CREATE TABLE IF NOT EXISTS event_consumer_offset (
    id BIGSERIAL PRIMARY KEY,
    
    source_type VARCHAR(32) NOT NULL,  -- PAIMON, ICEBERG, HUDI
    source_name VARCHAR(255) NOT NULL,
    offset_value VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(source_type, source_name)
);

-- Align databases created from earlier skeleton migrations with the entity model.
ALTER TABLE event_consumer_offset ADD COLUMN IF NOT EXISTS offset_value VARCHAR(255);
ALTER TABLE event_consumer_offset ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Add indexes to lakehouse_event table for Phase 2
ALTER TABLE lakehouse_event ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP;
CREATE INDEX IF NOT EXISTS idx_lakehouse_event_processed_at 
    ON lakehouse_event(processed_at) 
    WHERE processed_at IS NULL;

-- Add fields to workflow_instance for Phase 2
ALTER TABLE workflow_instance ADD COLUMN IF NOT EXISTS trigger_asset_key VARCHAR(255);
ALTER TABLE workflow_instance ADD COLUMN IF NOT EXISTS trigger_event_id VARCHAR(255);
ALTER TABLE workflow_instance ADD COLUMN IF NOT EXISTS trigger_snapshot_id VARCHAR(255);
ALTER TABLE trigger_history ADD COLUMN IF NOT EXISTS evaluation_payload_json JSONB;

CREATE INDEX IF NOT EXISTS idx_workflow_instance_trigger_asset 
    ON workflow_instance(trigger_asset_key);
