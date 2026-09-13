package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SchedulingTargetAdmissionStatuses;
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

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Persistent publication admission slot for one target asset and business date.
 *
 * Exactly one row exists for each {@code targetAssetKey + bizDate}. An active
 * holder prevents normal, backfill, recovery, and rerun paths from publishing
 * overlapping scheduling intents. This model is a scheduler-side lease and is
 * not an external execution lock.
 */
@Entity
@Table(
        name = "scheduling_target_admission",
        uniqueConstraints = {
            @UniqueConstraint(
                    name = "uk_target_admission_asset_date",
                    columnNames = {"target_asset_key", "biz_date"})
        },
        indexes = {
            @Index(name = "idx_target_admission_holder", columnList = "holder_task_instance_id"),
            @Index(name = "idx_target_admission_expiry", columnList = "status,expires_at ASC")
        }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SchedulingTargetAdmission {

    /** Database identifier of the reusable admission slot. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Optimistic version complementing pessimistic publication locking. */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /** Managed target asset protected by this slot. */
    @Column(name = "target_asset_key", nullable = false, updatable = false, length = 255)
    private String targetAssetKey;

    /** Business date protected by this slot. */
    @Column(name = "biz_date", nullable = false, updatable = false)
    private LocalDate bizDate;

    /** Task scheduling decision currently holding or most recently holding the slot. */
    @Column(name = "holder_task_instance_id")
    private Long holderTaskInstanceId;

    /** Immutable intent key currently holding or most recently holding the slot. */
    @Column(name = "holder_intent_key", length = 255)
    private String holderIntentKey;

    /** Trigger mode of the current or most recent holder. */
    @Column(name = "holder_trigger_type", length = 50)
    private String holderTriggerType;

    /** Scheduler-side slot state, either AVAILABLE or ACTIVE. */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /** Time at which the current holder acquired the publication lease. */
    @Column(name = "acquired_at")
    private LocalDateTime acquiredAt;

    /** Time after which another scheduling decision may reclaim the lease. */
    @Column(name = "expires_at")
    private LocalDateTime expiresAt;

    /** Time at which snapshot confirmation or timeout explicitly released the slot. */
    @Column(name = "released_at")
    private LocalDateTime releasedAt;

    /** Scheduler-side reason for the most recent explicit release. */
    @Column(name = "release_reason", columnDefinition = "TEXT")
    private String releaseReason;

    /** Record creation timestamp. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /** Record update timestamp. */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /** Initialize slot defaults and audit timestamps before insertion. */
    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        if (status == null) {
            status = SchedulingTargetAdmissionStatuses.AVAILABLE;
        }
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }

    /** Refresh the audit timestamp before changing the slot holder or state. */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
