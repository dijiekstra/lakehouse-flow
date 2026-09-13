package io.github.lakehouseflow.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Current scheduling truth of a data asset (table or partition).
 *
 * Asset state is updated monotonically:
 * - Newer snapshots/watermarks overwrite older ones
 * - Version is used for optimistic locking
 * - Quality and schema status track readiness
 *
 * This is the single source of truth for dependency evaluation.
 */
@Entity
@Table(
    name = "asset_state",
    indexes = {
        @Index(name = "idx_asset_key", columnList = "asset_key", unique = true),
        @Index(name = "idx_readiness", columnList = "readiness_status,updated_at DESC"),
        @Index(name = "idx_catalog_table", columnList = "catalog_name,database_name,table_name")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AssetState {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Unique asset key: catalog.database.table[.partition]
     * Example: paimon.prod.orders or paimon.prod.orders.dt=2025-09-11
     */
    @Column(name = "asset_key", nullable = false, unique = true, length = 255)
    private String assetKey;

    /**
     * Asset type: TABLE or PARTITION
     */
    @Column(name = "asset_type", nullable = false, length = 50)
    private String assetType;

    @Column(name = "catalog_name", nullable = false, length = 255)
    private String catalogName;

    @Column(name = "database_name", nullable = false, length = 255)
    private String databaseName;

    @Column(name = "table_name", nullable = false, length = 255)
    private String tableName;

    /**
     * Partition filter (e.g., dt=2025-09-11)
     */
    @Column(name = "partition_name", length = 255)
    private String partitionName;

    /**
     * Latest snapshot ID observed (monotonically increasing)
     */
    @Column(name = "latest_snapshot_id", length = 255)
    private String latestSnapshotId;

    /**
     * Latest schema ID observed
     */
    @Column(name = "latest_schema_id", length = 255)
    private String latestSchemaId;

    /**
     * Event time watermark (monotonically increasing)
     */
    @Column(name = "latest_watermark")
    private LocalDateTime latestWatermark;

    /**
     * When the latest snapshot was committed
     */
    @Column(name = "latest_commit_time")
    private LocalDateTime latestCommitTime;

    /**
     * Quality check result: PASSED, WARNING, FAILED, UNKNOWN
     */
    @Column(name = "quality_status", nullable = false, length = 50)
    private String qualityStatus;

    /**
     * Schema compatibility: UNKNOWN, COMPATIBLE, BREAKING_CHANGE, etc.
     */
    @Column(name = "schema_status", nullable = false, length = 50)
    private String schemaStatus;

    /**
     * Backfill status: NONE, IN_PROGRESS, COMPLETED
     */
    @Column(name = "backfill_status", nullable = false, length = 50)
    private String backfillStatus;

    /**
     * Overall readiness: UNKNOWN, NOT_READY, READY
     * Derived from snapshot + watermark + quality + schema conditions
     */
    @Column(name = "readiness_status", nullable = false, length = 50)
    private String readinessStatus;

    /**
     * Optimistic lock version for concurrent updates
     */
    @Column(name = "version", nullable = false)
    @Version
    private Long version;

    /**
     * Record update time
     */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /**
     * Record creation time
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /**
     * Initialize asset-state defaults and audit timestamps before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (createdAt == null) createdAt = LocalDateTime.now();
        if (updatedAt == null) updatedAt = LocalDateTime.now();
        if (qualityStatus == null) qualityStatus = "UNKNOWN";
        if (schemaStatus == null) schemaStatus = "UNKNOWN";
        if (backfillStatus == null) backfillStatus = "NONE";
        if (readinessStatus == null) readinessStatus = "UNKNOWN";
        if (version == null) version = 1L;
    }

    /**
     * Refresh the update timestamp before changing asset-state evidence.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
