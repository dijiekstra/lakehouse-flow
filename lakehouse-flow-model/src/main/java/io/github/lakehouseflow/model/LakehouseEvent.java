package io.github.lakehouseflow.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Raw event from lakehouse table formats (Paimon, Iceberg, Hudi).
 *
 * Events are ingested, deduplicated, and used to update asset state.
 * One event can affect multiple downstream workflows.
 */
@Entity
@Table(
    name = "lakehouse_event",
    indexes = {
        @Index(name = "idx_event_id", columnList = "event_id", unique = true),
        @Index(name = "idx_asset_snapshot", columnList = "catalog_name,database_name,table_name,partition_name,snapshot_id"),
        @Index(name = "idx_source_time", columnList = "source_type,observed_at DESC")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LakehouseEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Unique deterministic identifier for event deduplication.
     * Format: source_type + catalog + database + table + partition + event_type + snapshot_id
     */
    @Column(name = "event_id", nullable = false, unique = true, length = 255)
    private String eventId;

    /**
     * Event type: SNAPSHOT_COMPLETED, SCHEMA_CHANGED, QUALITY_RESULT, etc.
     */
    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    /**
     * Source type: PAIMON, ICEBERG, HUDI, MANUAL
     */
    @Column(name = "source_type", nullable = false, length = 50)
    private String sourceType;

    /**
     * Catalog name from lakehouse metadata
     */
    @Column(name = "catalog_name", nullable = false, length = 255)
    private String catalogName;

    /**
     * Database/schema name
     */
    @Column(name = "database_name", nullable = false, length = 255)
    private String databaseName;

    /**
     * Table name
     */
    @Column(name = "table_name", nullable = false, length = 255)
    private String tableName;

    /**
     * Partition name (e.g., dt=2025-09-11)
     */
    @Column(name = "partition_name", length = 255)
    private String partitionName;

    /**
     * Adapter-normalized monotonic snapshot coordinate used for scheduling comparisons.
     *
     * <p>When a format's native snapshot id is not ordered, the adapter stores the native value in
     * {@code payloadJson} and projects an ordered sequence or instant here.
     */
    @Column(name = "snapshot_id", length = 255)
    private String snapshotId;

    /**
     * Source-native schema identifier associated with this snapshot.
     */
    @Column(name = "schema_id", length = 255)
    private String schemaId;

    /**
     * Event time watermark for streaming semantics
     */
    @Column(name = "watermark")
    private LocalDateTime watermark;

    /**
     * Commit kind: APPEND, OVERWRITE, INCREMENTAL, etc.
     */
    @Column(name = "commit_kind", length = 50)
    private String commitKind;

    /**
     * When this snapshot was committed
     */
    @Column(name = "commit_time")
    private LocalDateTime commitTime;

    /**
     * Raw event metadata as JSON (for debugging and future extensions)
     */
    @Column(name = "payload_json", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> payloadJson;

    /**
     * When this event was observed by the scheduler
     */
    @Column(name = "observed_at", nullable = false)
    private LocalDateTime observedAt;

    /**
     * Record creation time
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /**
     * Record update time
     */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /**
     * Initialize audit timestamps and observed time before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (createdAt == null) createdAt = LocalDateTime.now();
        if (updatedAt == null) updatedAt = LocalDateTime.now();
        if (observedAt == null) observedAt = LocalDateTime.now();
    }

    /**
     * Refresh the update timestamp before changing the raw event record.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
