-- V6.0__instance_definition_links.sql
-- Link emitted scheduling instances back to published FlowPlan and node definitions.

ALTER TABLE workflow_instance
    ADD COLUMN IF NOT EXISTS flow_plan_version_id BIGINT REFERENCES flow_plan_version(id);

CREATE INDEX IF NOT EXISTS idx_workflow_instance_flow_plan_version
    ON workflow_instance(flow_plan_version_id);

ALTER TABLE task_instance
    ADD COLUMN IF NOT EXISTS flow_plan_version_id BIGINT REFERENCES flow_plan_version(id),
    ADD COLUMN IF NOT EXISTS schedule_node_id BIGINT REFERENCES schedule_node(id);

CREATE INDEX IF NOT EXISTS idx_task_instance_flow_plan_version
    ON task_instance(flow_plan_version_id);

CREATE INDEX IF NOT EXISTS idx_task_instance_schedule_node
    ON task_instance(schedule_node_id);
