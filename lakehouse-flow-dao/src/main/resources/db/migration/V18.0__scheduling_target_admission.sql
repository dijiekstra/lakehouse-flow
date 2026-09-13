-- V18.0__scheduling_target_admission.sql
-- Serialize scheduling-intent publication for one target asset and business date.

CREATE TABLE scheduling_target_admission (
    id BIGSERIAL PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0,
    target_asset_key VARCHAR(255) NOT NULL,
    biz_date DATE NOT NULL,
    holder_task_instance_id BIGINT,
    holder_intent_key VARCHAR(255),
    holder_trigger_type VARCHAR(50),
    status VARCHAR(32) NOT NULL DEFAULT 'AVAILABLE',
    acquired_at TIMESTAMP,
    expires_at TIMESTAMP,
    released_at TIMESTAMP,
    release_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_target_admission_asset_date UNIQUE (target_asset_key, biz_date),
    CONSTRAINT ck_target_admission_status CHECK (status IN ('AVAILABLE', 'ACTIVE'))
);

CREATE INDEX idx_target_admission_holder
    ON scheduling_target_admission(holder_task_instance_id);

CREATE INDEX idx_target_admission_expiry
    ON scheduling_target_admission(status, expires_at ASC);

-- Preserve a conservative lease for already-published decisions during upgrade.
-- Existing duplicate deliveries cannot be undone; the oldest holder wins the new slot.
INSERT INTO scheduling_target_admission (
    target_asset_key,
    biz_date,
    holder_task_instance_id,
    holder_intent_key,
    holder_trigger_type,
    status,
    acquired_at,
    expires_at,
    created_at,
    updated_at
)
SELECT DISTINCT ON (intent.target_asset_key, CAST(intent.biz_date AS DATE))
    intent.target_asset_key,
    CAST(intent.biz_date AS DATE),
    intent.task_instance_id,
    intent.intent_key,
    intent.trigger_type,
    'ACTIVE',
    COALESCE(task.scheduled_at, intent.created_at),
    GREATEST(COALESCE(task.scheduled_at, intent.created_at), CURRENT_TIMESTAMP) + INTERVAL '1 hour',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM scheduling_intent intent
JOIN task_instance task ON task.id = intent.task_instance_id
WHERE task.state = 'SCHEDULED'
ORDER BY
    intent.target_asset_key,
    CAST(intent.biz_date AS DATE),
    COALESCE(task.scheduled_at, intent.created_at) ASC,
    intent.id ASC;
