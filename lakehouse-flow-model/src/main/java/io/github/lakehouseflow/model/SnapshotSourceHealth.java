package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SnapshotSourceHealthOutcomes;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Durable table-level proof that a managed snapshot source was reconciled.
 *
 * <p>Snapshot confirmation consumes this record instead of calling a lake-format adapter.
 * A healthy record proves only the source range observed at {@code evidenceCheckedAt}; it is
 * not task runtime state and cannot confirm a target snapshot by itself.
 */
@Entity
@Table(
        name = "snapshot_source_health",
        uniqueConstraints = {
            @UniqueConstraint(name = "uk_snapshot_source_health_source", columnNames = {"source_type", "source_name"}),
            @UniqueConstraint(name = "uk_snapshot_source_health_asset", columnNames = "table_asset_key")
        },
        indexes = {
            @Index(name = "idx_snapshot_source_health_outcome", columnList = "outcome,evidence_checked_at DESC")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SnapshotSourceHealth {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Optimistic concurrency version for reconciliation writers. */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /** Lake format such as PAIMON. */
    @Column(name = "source_type", nullable = false, length = 50)
    private String sourceType;

    /** Stable source name used by the durable ingestion offset. */
    @Column(name = "source_name", nullable = false, length = 255)
    private String sourceName;

    /** Unique managed table key without a partition suffix. */
    @Column(name = "table_asset_key", nullable = false, length = 255)
    private String tableAssetKey;

    /** Persisted HEALTHY, REPAIRABLE, or BLOCKED reconciliation outcome. */
    @Column(name = "outcome", nullable = false, length = 32)
    private String outcome;

    /** Adapter-normalized relationship between source range and durable offset. */
    @Column(name = "offset_status", nullable = false, length = 64)
    private String offsetStatus;

    /** Relationship between the event ledger and AssetState projection. */
    @Column(name = "projection_status", nullable = false, length = 64)
    private String projectionStatus;

    /** Last source offset committed with its event and projection, or null. */
    @Column(name = "durable_offset", length = 255)
    private String durableOffset;

    /** Latest offset visible in the source during reconciliation, or null. */
    @Column(name = "latest_source_offset", length = 255)
    private String latestSourceOffset;

    /** Time of the actual source inspection represented by this proof. */
    @Column(name = "evidence_checked_at", nullable = false)
    private LocalDateTime evidenceCheckedAt;

    /** Human-readable reconciliation evidence for operations. */
    @Column(name = "detail", columnDefinition = "text")
    private String detail;

    /** Record update timestamp. */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /** Record creation timestamp. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /** Initialize validation, optimistic version, and audit timestamps before insert. */
    @PrePersist
    protected void onCreate() {
        outcome = SnapshotSourceHealthOutcomes.requirePersisted(outcome);
        if (version == null) {
            version = 0L;
        }
        LocalDateTime now = LocalDateTime.now();
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }

    /** Validate the new outcome and refresh the audit timestamp before update. */
    @PreUpdate
    protected void onUpdate() {
        outcome = SnapshotSourceHealthOutcomes.requirePersisted(outcome);
        updatedAt = LocalDateTime.now();
    }
}
