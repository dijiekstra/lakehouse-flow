package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.common.BackfillScopeTypes;
import io.github.lakehouseflow.common.BackfillSkipPolicies;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Persistent envelope for one scheduler-side backfill expansion.
 *
 * A batch records the historical date range, node scope, cascade policy, and
 * generated intent counts. It does not represent execution in an external
 * compute engine.
 */
@Entity
@Table(
    name = "backfill_batch",
    indexes = {
        @Index(name = "idx_backfill_batch_key", columnList = "batch_key", unique = true),
        @Index(name = "idx_backfill_batch_action_key", columnList = "action_key"),
        @Index(name = "uk_backfill_batch_source", columnList = "source_backfill_batch_id", unique = true),
        @Index(name = "idx_backfill_batch_flow_plan_version", columnList = "flow_plan_version_id"),
        @Index(name = "idx_backfill_batch_status", columnList = "status,created_at DESC")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BackfillBatch {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Stable idempotency key for this batch, normally the action key.
     */
    @Column(name = "batch_key", nullable = false, unique = true, length = 255)
    private String batchKey;

    /**
     * Scheduling action key that requested this batch.
     */
    @Column(name = "action_key", nullable = false, length = 255)
    private String actionKey;

    /**
     * Published FlowPlanVersion used to expand this batch.
     */
    @Column(name = "flow_plan_version_id", nullable = false)
    private Long flowPlanVersionId;

    /**
     * Workflow code copied from the published FlowPlanVersion.
     */
    @Column(name = "workflow_code", nullable = false, length = 255)
    private String workflowCode;

    /**
     * Workflow version copied from the published FlowPlanVersion.
     */
    @Column(name = "workflow_version", nullable = false)
    private Integer workflowVersion;

    /**
     * Immutable batch scope: complete FlowPlan graph or one selected node subgraph.
     */
    @Column(name = "scope_type", nullable = false, length = 64)
    private String scopeType;

    /**
     * Node codes allowed to bypass graph parents as explicit entries for each date.
     */
    @Column(name = "entry_node_codes_json", nullable = false, columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<String> entryNodeCodes;

    /**
     * Immutable node codes selected for every business date in this batch.
     */
    @Column(name = "selected_node_codes_json", nullable = false, columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<String> selectedNodeCodes;

    /**
     * ScheduleNode where a NODE_SUBGRAPH backfill starts; null for FULL_FLOW.
     */
    @Column(name = "start_schedule_node_id")
    private Long startScheduleNodeId;

    /**
     * Node code where a NODE_SUBGRAPH backfill starts; null for FULL_FLOW.
     */
    @Column(name = "start_node_code", length = 255)
    private String startNodeCode;

    /**
     * Inclusive start business date.
     */
    @Column(name = "biz_date_start", nullable = false)
    private LocalDate bizDateStart;

    /**
     * Inclusive end business date.
     */
    @Column(name = "biz_date_end", nullable = false)
    private LocalDate bizDateEnd;

    /**
     * Node cascade policy used by NODE_SUBGRAPH; null for FULL_FLOW.
     */
    @Column(name = "cascade_policy", length = 64)
    private String cascadePolicy;

    /**
     * Business-date progression mode used to expose scheduling intents.
     */
    @Column(name = "progression_mode", nullable = false, length = 64)
    private String progressionMode;

    /**
     * Maximum number of active business dates for a limited progression mode.
     */
    @Column(name = "max_active_dates")
    private Integer maxActiveDates;

    /**
     * Policy controlling whether fully snapshot-confirmed dates may be omitted.
     */
    @Column(name = "skip_policy", nullable = false, length = 64)
    private String skipPolicy;

    /**
     * Number of complete business dates omitted using durable confirmation evidence.
     */
    @Column(name = "skipped_date_count", nullable = false)
    private Integer skippedDateCount;

    /**
     * Date-keyed map of node codes to historical task ids proving each skip decision.
     */
    @Column(name = "skip_evidence_json", nullable = false, columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> skipEvidenceJson;

    /**
     * Failed batch that this recovery batch replaces, or null for an original request.
     */
    @Column(name = "source_backfill_batch_id")
    private Long sourceBackfillBatchId;

    /**
     * Recovery depth in the replacement chain; original batches use zero.
     */
    @Column(name = "recovery_attempt", nullable = false)
    private Integer recoveryAttempt;

    /**
     * Recovery strategy used by a replacement batch, or null for an original batch.
     */
    @Column(name = "recovery_strategy", length = 64)
    private String recoveryStrategy;

    /**
     * Scheduler-side batch state. See {@link BackfillBatchStatuses}.
     */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /**
     * Number of workflow wrappers generated for this batch.
     */
    @Column(name = "produced_workflow_count", nullable = false)
    private Integer producedWorkflowCount;

    /**
     * Number of task intents generated for this batch.
     */
    @Column(name = "total_item_count", nullable = false)
    private Integer totalItemCount;

    /**
     * User or system identity that requested the batch.
     */
    @Column(name = "requested_by", length = 255)
    private String requestedBy;

    /**
     * Human-readable request reason.
     */
    @Column(name = "reason", columnDefinition = "TEXT")
    private String reason;

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
     * Initialize batch defaults and audit timestamps before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (status == null) {
            status = BackfillBatchStatuses.CREATED;
        }
        if (progressionMode == null) {
            progressionMode = BackfillProgressionModes.PARALLEL;
        }
        if (scopeType == null) {
            scopeType = BackfillScopeTypes.NODE_SUBGRAPH;
        }
        if (entryNodeCodes == null) {
            entryNodeCodes = startNodeCode == null ? List.of() : List.of(startNodeCode);
        }
        if (selectedNodeCodes == null) {
            selectedNodeCodes = BackfillScopeTypes.NODE_SUBGRAPH.equals(scopeType)
                    ? entryNodeCodes
                    : List.of();
        }
        if (skipPolicy == null) {
            skipPolicy = BackfillSkipPolicies.NONE;
        }
        if (skippedDateCount == null) {
            skippedDateCount = 0;
        }
        if (skipEvidenceJson == null) {
            skipEvidenceJson = Map.of();
        }
        if (producedWorkflowCount == null) {
            producedWorkflowCount = 0;
        }
        if (recoveryAttempt == null) {
            recoveryAttempt = 0;
        }
        if (totalItemCount == null) {
            totalItemCount = 0;
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now();
        }
    }

    /**
     * Refresh the update timestamp before changing batch progress metadata.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
