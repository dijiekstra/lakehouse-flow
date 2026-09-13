package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SchedulingActionStatuses;
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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Durable audit record for one scheduling-side action request.
 *
 * Actions model operator/system intent such as rerun, backfill, cancel, skip, or
 * snapshot recheck. They affect Lakehouse Flow scheduling records only and do
 * not execute external compute.
 */
@Entity
@Table(
    name = "scheduling_action",
    indexes = {
        @Index(name = "idx_scheduling_action_key", columnList = "action_key", unique = true),
        @Index(name = "idx_scheduling_action_type", columnList = "action_type,created_at DESC"),
        @Index(name = "idx_scheduling_action_status", columnList = "status,created_at ASC"),
        @Index(name = "idx_scheduling_action_workflow_instance", columnList = "workflow_instance_id"),
        @Index(name = "idx_scheduling_action_task_instance", columnList = "task_instance_id"),
        @Index(name = "idx_scheduling_action_workflow_created", columnList = "workflow_code,created_at DESC"),
        @Index(name = "idx_scheduling_action_flow_plan_version", columnList = "flow_plan_version_id"),
        @Index(name = "idx_scheduling_action_schedule_node", columnList = "schedule_node_id")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SchedulingAction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Caller-provided idempotency key for the action request.
     */
    @Column(name = "action_key", nullable = false, unique = true, length = 255)
    private String actionKey;

    /**
     * Type of scheduling action.
     */
    @Column(name = "action_type", nullable = false, length = 64)
    private String actionType;

    /**
     * Target scope type such as WORKFLOW_INSTANCE, TASK_INSTANCE, or WORKFLOW_DEFINITION.
     */
    @Column(name = "scope_type", nullable = false, length = 64)
    private String scopeType;

    /**
     * Workflow definition code when the action targets a workflow.
     */
    @Column(name = "workflow_code", length = 255)
    private String workflowCode;

    /**
     * Workflow definition version when the action targets a workflow.
     */
    @Column(name = "workflow_version")
    private Integer workflowVersion;

    /**
     * Existing workflow instance targeted by the action.
     */
    @Column(name = "workflow_instance_id")
    private Long workflowInstanceId;

    /**
     * Existing task instance targeted by the action.
     */
    @Column(name = "task_instance_id")
    private Long taskInstanceId;

    /**
     * FlowPlanVersion targeted by node-level actions.
     */
    @Column(name = "flow_plan_version_id")
    private Long flowPlanVersionId;

    /**
     * ScheduleNode targeted by node-level actions.
     */
    @Column(name = "schedule_node_id")
    private Long scheduleNodeId;

    /**
     * Inclusive start business date for range actions such as backfill.
     */
    @Column(name = "biz_date_start")
    private LocalDate bizDateStart;

    /**
     * Inclusive end business date for range actions such as backfill.
     */
    @Column(name = "biz_date_end")
    private LocalDate bizDateEnd;

    /**
     * Target asset key used by snapshot confirmation actions.
     */
    @Column(name = "target_asset_key", length = 255)
    private String targetAssetKey;

    /**
     * Target snapshot id supplied by the caller when applicable.
     */
    @Column(name = "target_snapshot_id", length = 255)
    private String targetSnapshotId;

    /**
     * User or system identity that requested this action.
     */
    @Column(name = "requested_by", length = 255)
    private String requestedBy;

    /**
     * Human-readable reason for the action request.
     */
    @Column(name = "reason", columnDefinition = "TEXT")
    private String reason;

    /**
     * Processing status of the action request.
     */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /**
     * Human-readable processing result.
     */
    @Column(name = "result_message", columnDefinition = "TEXT")
    private String resultMessage;

    /**
     * Primary workflow instance emitted by the action when applicable.
     */
    @Column(name = "produced_workflow_instance_id")
    private Long producedWorkflowInstanceId;

    /**
     * Primary task instance affected by the action when applicable.
     */
    @Column(name = "produced_task_instance_id")
    private Long producedTaskInstanceId;

    /**
     * JSON payload preserving action-specific request details.
     */
    @Column(name = "request_payload_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> requestPayloadJson;

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
     * Initialize action defaults and audit timestamps before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (status == null) {
            status = SchedulingActionStatuses.ACCEPTED;
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now();
        }
    }

    /**
     * Refresh the update timestamp before changing action processing state.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
