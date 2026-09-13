package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SchedulingStates;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * One scheduling decision for a task.
 *
 * State machine:
 * CREATED → WAITING_SNAPSHOT → READY_TO_SCHEDULE → SCHEDULED → SNAPSHOT_CONFIRMED
 *                                                    ↘ SKIPPED   ↘ SNAPSHOT_NOT_ADVANCED
 *
 * These states describe scheduling/audit only. Execution, executor attempts,
 * resource queues, retries, and runtime callbacks are outside Lakehouse Flow.
 *
 * Unique key: workflow_instance_id + task_code
 */
@Entity
@Table(
    name = "task_instance",
    indexes = {
        @Index(name = "idx_task_instance_key", columnList = "instance_key", unique = true),
        @Index(name = "idx_workflow_task", columnList = "workflow_instance_id,task_code"),
        @Index(name = "idx_task_flow_plan_version", columnList = "flow_plan_version_id"),
        @Index(name = "idx_task_schedule_node", columnList = "schedule_node_id"),
        @Index(name = "idx_task_state", columnList = "state,updated_at DESC"),
        @Index(name = "idx_task_target_asset", columnList = "target_asset_key")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskInstance {

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
     * Unique instance key within workflow instance
     * Format: workflow_instance_id:task_code
     */
    @Column(name = "instance_key", nullable = false, unique = true, length = 255)
    private String instanceKey;

    /**
     * Foreign key to workflow_instance
     */
    @Column(name = "workflow_instance_id", nullable = false)
    private Long workflowInstanceId;

    /**
     * Task code (e.g., "data_quality_check")
     */
    @Column(name = "task_code", nullable = false, length = 255)
    private String taskCode;

    /**
     * Task definition version
     */
    @Column(name = "task_version", nullable = false)
    private Integer taskVersion;

    /**
     * FlowPlanVersion that produced this task scheduling instance.
     */
    @Column(name = "flow_plan_version_id")
    private Long flowPlanVersionId;

    /**
     * ScheduleNode that produced this task scheduling instance.
     */
    @Column(name = "schedule_node_id")
    private Long scheduleNodeId;

    /**
     * Business date
     */
    @Column(name = "biz_date", nullable = false)
    private LocalDateTime bizDate;

    /**
     * Scheduling state. See {@link SchedulingStates}.
     */
    @Column(name = "state", nullable = false, length = 50)
    private String state;

    /**
     * Reason why task is waiting for snapshot evidence.
     * Example: "Waiting for: paimon.prod.orders.dt=2025-09-11 (no snapshot yet)"
     */
    @Column(name = "waiting_reason", columnDefinition = "text")
    private String waitingReason;

    /**
     * Target asset whose snapshot progress confirms the scheduling result.
     */
    @Column(name = "target_asset_key", length = 255)
    private String targetAssetKey;

    /**
     * Target snapshot observed before Lakehouse Flow emitted the scheduling decision.
     */
    @Column(name = "baseline_snapshot_id", length = 255)
    private String baselineSnapshotId;

    /**
     * Snapshot observed when Lakehouse Flow checked the target asset.
     */
    @Column(name = "observed_snapshot_id", length = 255)
    private String observedSnapshotId;

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
     * Refresh the update timestamp before changing scheduling evidence.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
