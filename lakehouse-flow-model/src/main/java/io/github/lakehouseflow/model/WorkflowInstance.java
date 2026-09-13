package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SchedulingStates;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * One scheduling decision for a workflow.
 *
 * State machine:
 * CREATED → WAITING_SNAPSHOT → READY_TO_SCHEDULE → SCHEDULED → SNAPSHOT_CONFIRMED
 *                                                    ↘ CANCELLED ↘ SNAPSHOT_NOT_ADVANCED
 *
 * These states describe scheduling/audit only. Lakehouse Flow confirms the
 * result from target snapshot progress, not task runtime callbacks.
 *
 * Unique key: workflow_code + workflow_version + biz_date + trigger_type + trigger_asset_key + trigger_snapshot_id
 */
@Entity
@Table(
    name = "workflow_instance",
    indexes = {
        @Index(name = "idx_instance_key", columnList = "instance_key", unique = true),
        @Index(name = "idx_workflow", columnList = "workflow_code,workflow_version"),
        @Index(name = "idx_workflow_flow_plan_version", columnList = "flow_plan_version_id"),
        @Index(name = "idx_state", columnList = "state,updated_at DESC"),
        @Index(name = "idx_biz_date", columnList = "biz_date DESC")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WorkflowInstance {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Optimistic concurrency version for scheduler-side state transitions.
     */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /**
     * Unique instance key for idempotency
     * Format: workflow_code:workflow_version:biz_date:trigger_type:trigger_asset_key:trigger_snapshot_id
     */
    @Column(name = "instance_key", nullable = false, unique = true, length = 255)
    private String instanceKey;

    /**
     * Workflow code (e.g., "dwd_order_agg")
     */
    @Column(name = "workflow_code", nullable = false, length = 255)
    private String workflowCode;

    /**
     * Workflow version
     */
    @Column(name = "workflow_version", nullable = false)
    private Integer workflowVersion;

    /**
     * FlowPlanVersion that produced this workflow scheduling instance.
     */
    @Column(name = "flow_plan_version_id")
    private Long flowPlanVersionId;

    /**
     * Business date for this workflow run (e.g., 2025-09-11)
     */
    @Column(name = "biz_date", nullable = false)
    private LocalDateTime bizDate;

    /**
     * Trigger type: SNAPSHOT, SCHEDULE, BACKFILL, MANUAL
     */
    @Column(name = "trigger_type", nullable = false, length = 50)
    private String triggerType;

    /**
     * Which event triggered this instance (event_id from lakehouse_event)
     */
    @Column(name = "trigger_event_id", length = 255)
    private String triggerEventId;

    /**
     * Human-readable reason why this instance was created
     */
    @Column(name = "trigger_reason", columnDefinition = "text")
    private String triggerReason;

    /**
     * Scheduling state. See {@link SchedulingStates}.
     */
    @Column(name = "state", nullable = false, length = 50)
    private String state;

    /**
     * When Lakehouse Flow emitted the scheduling decision.
     */
    @Column(name = "scheduled_at")
    private LocalDateTime scheduledAt;

    /**
     * When target snapshot evidence was last checked.
     */
    @Column(name = "last_snapshot_check_at")
    private LocalDateTime lastSnapshotCheckAt;

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
     * Initialize audit fields and the default scheduling state before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (createdAt == null) createdAt = LocalDateTime.now();
        if (updatedAt == null) updatedAt = LocalDateTime.now();
        if (state == null) state = SchedulingStates.CREATED;
    }

    /**
     * Refresh the update timestamp before changing workflow scheduling evidence.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
