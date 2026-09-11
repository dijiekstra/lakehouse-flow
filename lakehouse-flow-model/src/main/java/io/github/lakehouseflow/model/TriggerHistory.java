package io.github.lakehouseflow.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.type.descriptor.jdbc.JsonJdbcType;

import java.time.LocalDateTime;

/**
 * Trigger History - Audit log for workflow and task instance creation.
 *
 * Records why a workflow or task instance was created, including:
 * - Which event triggered it
 * - Which dependency conditions were evaluated
 * - Whether the creation was allowed or skipped
 *
 * Used for:
 * - Auditing (why was this instance created?)
 * - Debugging (why is this instance not running?)
 * - Tracing (what snapshot led to this task?)
 */
@Entity
@Table(
    name = "trigger_history",
    indexes = {
        @Index(name = "idx_trigger_key", columnList = "trigger_key", unique = true),
        @Index(name = "idx_workflow_instance_id", columnList = "workflow_instance_id"),
        @Index(name = "idx_task_instance_id", columnList = "task_instance_id"),
        @Index(name = "idx_asset_key", columnList = "asset_key"),
        @Index(name = "idx_created_at", columnList = "created_at")
    }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TriggerHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Unique trigger key for idempotency.
     * Format: workflow_code:version:biz_date:trigger_id:snapshot_id
     */
    @Column(name = "trigger_key", nullable = false, length = 255)
    private String triggerKey;

    /**
     * Type of trigger: SNAPSHOT_DRIVEN, SCHEDULED, MANUAL, BACKFILL
     */
    @Column(name = "trigger_type", nullable = false, length = 32)
    private String triggerType;

    /**
     * Asset that triggered this workflow/task.
     * E.g., "paimon.prod.ods.orders.dt=2026-09-10"
     */
    @Column(name = "asset_key", length = 255)
    private String assetKey;

    /**
     * Snapshot ID of the asset that triggered.
     */
    @Column(name = "snapshot_id", length = 255)
    private String snapshotId;

    /**
     * Watermark of the asset snapshot.
     */
    @Column(name = "watermark")
    private LocalDateTime watermark;

    /**
     * Original event ID that triggered this.
     */
    @Column(name = "event_id", length = 255)
    private String eventId;

    /**
     * The workflow instance created by this trigger.
     */
    @Column(name = "workflow_instance_id")
    private Long workflowInstanceId;

    /**
     * The task instance created by this trigger.
     */
    @Column(name = "task_instance_id")
    private Long taskInstanceId;

    /**
     * Decision: TRIGGERED or SKIPPED
     */
    @Column(name = "decision", nullable = false, length = 32)
    private String decision;

    /**
     * Human-readable reason for the decision.
     * E.g., "Snapshot 1000 >= required 950" or "Asset not ready, waiting for watermark >= 2026-09-11T23:59"
     */
    @Column(name = "decision_reason", columnDefinition = "TEXT")
    private String decisionReason;

    /**
     * JSON payload with detailed evaluation results (optional).
     * Can contain condition evaluation details, snapshots, watermarks, etc.
     */
    @Column(name = "evaluation_payload_json", columnDefinition = "JSONB")
    private String evaluationPayloadJson;

    /**
     * Timestamp when this trigger record was created.
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }

    @Override
    public String toString() {
        return "TriggerHistory{" +
                "id=" + id +
                ", triggerKey='" + triggerKey + '\'' +
                ", triggerType='" + triggerType + '\'' +
                ", assetKey='" + assetKey + '\'' +
                ", snapshotId='" + snapshotId + '\'' +
                ", decision='" + decision + '\'' +
                ", workflowInstanceId=" + workflowInstanceId +
                ", taskInstanceId=" + taskInstanceId +
                '}';
    }
}
