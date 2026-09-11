package io.github.lakehouseflow.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * One execution of a workflow.
 *
 * State machine:
 * CREATED → WAITING → RUNNING → SUCCESS
 *                  ↘ FAILED
 *                  ↘ TIMEOUT
 *                  ↘ CANCELLED
 *
 * Unique key: workflow_code + workflow_version + biz_date + trigger_type + trigger_asset_key + trigger_snapshot_id
 */
@Entity
@Table(
    name = "workflow_instance",
    indexes = {
        @Index(name = "idx_instance_key", columnList = "instance_key", unique = true),
        @Index(name = "idx_workflow", columnList = "workflow_code,workflow_version"),
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
     * Instance state: CREATED, WAITING, RUNNING, SUCCESS, FAILED, TIMEOUT, CANCELLED
     */
    @Column(name = "state", nullable = false, length = 50)
    private String state;

    /**
     * When execution started (transition to RUNNING)
     */
    @Column(name = "start_time")
    private LocalDateTime startTime;

    /**
     * When execution ended
     */
    @Column(name = "end_time")
    private LocalDateTime endTime;

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

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) createdAt = LocalDateTime.now();
        if (updatedAt == null) updatedAt = LocalDateTime.now();
        if (state == null) state = "CREATED";
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
