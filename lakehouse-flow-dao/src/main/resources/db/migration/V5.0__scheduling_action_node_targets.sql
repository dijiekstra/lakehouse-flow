-- V5.0__scheduling_action_node_targets.sql
-- Link scheduling actions to FlowPlanVersion and ScheduleNode targets.

ALTER TABLE scheduling_action
    ADD COLUMN IF NOT EXISTS flow_plan_version_id BIGINT REFERENCES flow_plan_version(id),
    ADD COLUMN IF NOT EXISTS schedule_node_id BIGINT REFERENCES schedule_node(id);

CREATE INDEX IF NOT EXISTS idx_scheduling_action_flow_plan_version
    ON scheduling_action(flow_plan_version_id);

CREATE INDEX IF NOT EXISTS idx_scheduling_action_schedule_node
    ON scheduling_action(schedule_node_id);
