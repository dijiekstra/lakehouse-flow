package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.FlowPlanStatuses;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Scheduler-side Flow definition and isolation boundary.
 *
 * A FlowPlan groups versions, nodes, ownership, and future authorization policy.
 * It does not represent an executing workflow. Scheduling instances reference a
 * published FlowPlan version when Lakehouse Flow emits scheduling intent.
 */
@Entity
@Table(
    name = "flow_plan",
    indexes = {
        @Index(name = "idx_flow_plan_code", columnList = "flow_code", unique = true),
        @Index(name = "idx_flow_plan_status", columnList = "status,updated_at DESC"),
        @Index(name = "idx_flow_plan_space", columnList = "flow_space_code,updated_at DESC"),
        @Index(name = "idx_flow_plan_owner", columnList = "owner,updated_at DESC")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FlowPlan {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Stable Flow code used as the external definition identity.
     */
    @Column(name = "flow_code", nullable = false, unique = true, length = 255)
    private String flowCode;

    /**
     * Human-readable Flow name.
     */
    @Column(name = "flow_name", nullable = false, length = 255)
    private String flowName;

    /**
     * Lightweight sharing boundary for multi-user use cases.
     */
    @Column(name = "flow_space_code", length = 255)
    private String flowSpaceCode;

    /**
     * User or system that owns this FlowPlan.
     */
    @Column(name = "owner", length = 255)
    private String owner;

    /**
     * Human-readable Flow description.
     */
    @Column(name = "description", columnDefinition = "TEXT")
    private String description;

    /**
     * FlowPlan lifecycle status.
     */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /**
     * Currently published version number, or zero when no version is published.
     */
    @Column(name = "current_version", nullable = false)
    private Integer currentVersion;

    /**
     * Record creation timestamp.
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /**
     * Record update timestamp.
     */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /**
     * Mark this FlowPlan as published at a specific version.
     *
     * @param version published FlowPlan version
     */
    public void markPublished(Integer version) {
        status = FlowPlanStatuses.PUBLISHED;
        currentVersion = version;
    }

    /**
     * Initialize default lifecycle and audit timestamps before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (status == null) {
            status = FlowPlanStatuses.DRAFT;
        }
        if (currentVersion == null) {
            currentVersion = 0;
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now();
        }
    }

    /**
     * Refresh the update timestamp before changing the FlowPlan definition state.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
