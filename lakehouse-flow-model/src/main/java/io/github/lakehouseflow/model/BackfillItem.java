package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.BackfillItemStatuses;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One node/date item produced by a backfill batch.
 *
 * The item links a historical business date and ScheduleNode to the generated
 * scheduler-side workflow/task intent. It does not represent executor runtime.
 */
@Entity
@Table(
    name = "backfill_item",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_backfill_item_batch_node_date", columnNames = {
            "backfill_batch_id",
            "biz_date",
            "node_code"
        }),
        @UniqueConstraint(name = "uk_backfill_item_task_instance", columnNames = "task_instance_id")
    },
    indexes = {
        @Index(name = "idx_backfill_item_batch", columnList = "backfill_batch_id,biz_date ASC"),
        @Index(name = "idx_backfill_item_flow_plan_version", columnList = "flow_plan_version_id"),
        @Index(name = "idx_backfill_item_schedule_node", columnList = "schedule_node_id"),
        @Index(name = "idx_backfill_item_status", columnList = "status,created_at ASC")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BackfillItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Owning BackfillBatch id.
     */
    @Column(name = "backfill_batch_id", nullable = false)
    private Long backfillBatchId;

    /**
     * Business date covered by this item.
     */
    @Column(name = "biz_date", nullable = false)
    private LocalDate bizDate;

    /**
     * Published FlowPlanVersion used to generate this item.
     */
    @Column(name = "flow_plan_version_id", nullable = false)
    private Long flowPlanVersionId;

    /**
     * ScheduleNode generated for this item.
     */
    @Column(name = "schedule_node_id", nullable = false)
    private Long scheduleNodeId;

    /**
     * Node code generated for this item.
     */
    @Column(name = "node_code", nullable = false, length = 255)
    private String nodeCode;

    /**
     * Target asset expected to advance after the intent is delivered.
     */
    @Column(name = "target_asset_key", length = 255)
    private String targetAssetKey;

    /**
     * Workflow wrapper generated for this item.
     */
    @Column(name = "workflow_instance_id", nullable = false)
    private Long workflowInstanceId;

    /**
     * Task scheduling intent generated for this item.
     */
    @Column(name = "task_instance_id", nullable = false)
    private Long taskInstanceId;

    /**
     * Scheduler-side item state. See {@link BackfillItemStatuses}.
     */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

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
     * Initialize item defaults and audit timestamps before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (status == null) {
            status = BackfillItemStatuses.CREATED;
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now();
        }
    }

    /**
     * Refresh the update timestamp before changing item metadata.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
