-- Unify complete-Flow and node-subgraph backfills under one immutable batch scope.

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS scope_type VARCHAR(64) NOT NULL DEFAULT 'NODE_SUBGRAPH';

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS entry_node_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE backfill_batch
    ADD COLUMN IF NOT EXISTS selected_node_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE backfill_batch
SET entry_node_codes_json = jsonb_build_array(start_node_code)
WHERE jsonb_array_length(entry_node_codes_json) = 0
  AND start_node_code IS NOT NULL;

UPDATE backfill_batch AS batch
SET selected_node_codes_json = COALESCE(
    (
        SELECT jsonb_agg(DISTINCT item.node_code ORDER BY item.node_code)
        FROM backfill_item AS item
        WHERE item.backfill_batch_id = batch.id
    ),
    batch.entry_node_codes_json
)
WHERE jsonb_array_length(batch.selected_node_codes_json) = 0;

ALTER TABLE backfill_batch
    ALTER COLUMN start_schedule_node_id DROP NOT NULL;

ALTER TABLE backfill_batch
    ALTER COLUMN start_node_code DROP NOT NULL;

ALTER TABLE backfill_batch
    ALTER COLUMN cascade_policy DROP NOT NULL;

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_scope_type
        CHECK (scope_type IN ('FULL_FLOW', 'NODE_SUBGRAPH'));

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_scope_fields
        CHECK (
            (scope_type = 'FULL_FLOW'
                AND start_schedule_node_id IS NULL
                AND start_node_code IS NULL
                AND cascade_policy IS NULL)
            OR (scope_type = 'NODE_SUBGRAPH'
                AND start_schedule_node_id IS NOT NULL
                AND start_node_code IS NOT NULL
                AND cascade_policy IS NOT NULL)
        );

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_entry_nodes
        CHECK (
            jsonb_typeof(entry_node_codes_json) = 'array'
            AND jsonb_array_length(entry_node_codes_json) > 0
        );

ALTER TABLE backfill_batch
    ADD CONSTRAINT ck_backfill_batch_selected_nodes
        CHECK (
            jsonb_typeof(selected_node_codes_json) = 'array'
            AND jsonb_array_length(selected_node_codes_json) > 0
        );

ALTER TABLE backfill_batch
    ALTER COLUMN entry_node_codes_json DROP DEFAULT;

ALTER TABLE backfill_batch
    ALTER COLUMN selected_node_codes_json DROP DEFAULT;
