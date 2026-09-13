-- Preserve every physical snapshot for source continuity while keeping business-data progress separate.
ALTER TABLE lakehouse_event
    ADD COLUMN data_change BOOLEAN;

-- Promote explicit format-neutral payload evidence first, regardless of the source adapter.
UPDATE lakehouse_event
SET data_change = (payload_json ->> 'dataChange')::BOOLEAN
WHERE data_change IS NULL
  AND jsonb_typeof(payload_json -> 'dataChange') = 'boolean';

-- Legacy Paimon events have a deterministic compatibility mapping. Unknown formats stay null and fail closed.
UPDATE lakehouse_event
SET data_change = CASE
    WHEN UPPER(commit_kind) IN ('APPEND', 'OVERWRITE') THEN true
    WHEN UPPER(commit_kind) IN ('COMPACT', 'ANALYZE') THEN false
    ELSE NULL
END
WHERE data_change IS NULL
  AND UPPER(source_type) = 'PAIMON';

CREATE INDEX idx_lakehouse_event_source_data
    ON lakehouse_event(source_type, catalog_name, database_name, table_name, data_change, observed_at DESC, id DESC);

ALTER TABLE asset_state
    ADD COLUMN latest_data_snapshot_id VARCHAR(255);

ALTER TABLE asset_state
    ADD COLUMN latest_data_watermark TIMESTAMP;

ALTER TABLE asset_state
    ADD COLUMN latest_data_commit_time TIMESTAMP;

-- Rebuild table and partition data tracks only from durable events that were classified above.
-- Never copy latest_snapshot_id because it may point at a maintenance snapshot.
WITH data_projection AS (
    SELECT asset.id AS asset_state_id,
           (ARRAY_AGG(event.snapshot_id ORDER BY event.observed_at DESC, event.id DESC)
               FILTER (WHERE event.snapshot_id IS NOT NULL))[1] AS snapshot_id,
           MAX(event.watermark) AS watermark,
           MAX(event.commit_time) AS commit_time
    FROM asset_state asset
    JOIN lakehouse_event event
      ON event.catalog_name = asset.catalog_name
     AND event.database_name = asset.database_name
     AND event.table_name = asset.table_name
     AND event.data_change = true
     AND (
          asset.partition_name IS NULL
          OR event.partition_name = asset.partition_name
          OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements_text(
                  CASE
                      WHEN jsonb_typeof(event.payload_json -> 'changedPartitions') = 'array'
                          THEN event.payload_json -> 'changedPartitions'
                      ELSE '[]'::jsonb
                  END
              ) AS changed_partition(value)
              WHERE changed_partition.value = asset.partition_name
          )
     )
    GROUP BY asset.id
)
UPDATE asset_state asset
SET latest_data_snapshot_id = projection.snapshot_id,
    latest_data_watermark = projection.watermark,
    latest_data_commit_time = projection.commit_time,
    version = asset.version + 1,
    updated_at = CURRENT_TIMESTAMP
FROM data_projection projection
WHERE asset.id = projection.asset_state_id;

-- The reconciliation loop repairs any remaining typed-event projection drift after startup.
