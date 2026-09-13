-- V13.0__scheduling_action_query_index.sql
-- Support recent action audit searches within one workflow definition.

CREATE INDEX IF NOT EXISTS idx_scheduling_action_workflow_created
    ON scheduling_action(workflow_code, created_at DESC);
