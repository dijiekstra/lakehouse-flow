package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.common.ScheduleNodeTypes;
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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Node definition inside a FlowPlan version.
 *
 * ScheduleNode captures dependency edges, input asset requirements, and output
 * asset confirmation policy. It is a scheduling graph node only; Lakehouse Flow
 * still delegates any real work to downstream systems.
 */
@Entity
@Table(
    name = "schedule_node",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_schedule_node_version_code", columnNames = {"flow_plan_version_id", "node_code"})
    },
    indexes = {
        @Index(name = "idx_schedule_node_version", columnList = "flow_plan_version_id,sort_order ASC"),
        @Index(name = "idx_schedule_node_code", columnList = "node_code"),
        @Index(name = "idx_schedule_node_output_asset", columnList = "output_asset_key")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScheduleNode {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Owning FlowPlanVersion id.
     */
    @Column(name = "flow_plan_version_id", nullable = false)
    private Long flowPlanVersionId;

    /**
     * Stable node code unique within the version.
     */
    @Column(name = "node_code", nullable = false, length = 255)
    private String nodeCode;

    /**
     * Human-readable node name.
     */
    @Column(name = "node_name", nullable = false, length = 255)
    private String nodeName;

    /**
     * Scheduler-side node type. See {@link ScheduleNodeTypes}.
     */
    @Column(name = "node_type", nullable = false, length = 64)
    private String nodeType;

    /**
     * Engine-neutral activation model used by DAG scheduling.
     */
    @Builder.Default
    @Column(name = "processing_mode", nullable = false, length = 32)
    private String processingMode = ScheduleNodeProcessingModes.BATCH;

    /**
     * Upstream node codes within the same FlowPlan version.
     */
    @Column(name = "depends_on_nodes_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<String> dependsOnNodes;

    /**
     * Input asset dependency conditions evaluated before releasing this node.
     */
    @Column(name = "input_dependency_spec_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> inputDependencySpecJson;

    /**
     * Target asset that should advance after downstream systems consume this node intent.
     */
    @Column(name = "output_asset_key", length = 255)
    private String outputAssetKey;

    /**
     * Node-level snapshot confirmation policy.
     */
    @Column(name = "confirmation_policy_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> confirmationPolicyJson;

    /**
     * Stable ordering hint for deterministic graph presentation and scans.
     */
    @Column(name = "sort_order", nullable = false)
    private Integer sortOrder;

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
     * Check whether this node declares an output asset for snapshot confirmation.
     *
     * @return true when the output asset key is present
     */
    public boolean hasOutputAsset() {
        return outputAssetKey != null && !outputAssetKey.isBlank();
    }

    /**
     * Initialize default JSON values, sort order, and audit timestamps before insert.
     */
    @PrePersist
    protected void onCreate() {
        processingMode = ScheduleNodeProcessingModes.normalize(processingMode);
        if (dependsOnNodes == null) {
            dependsOnNodes = List.of();
        }
        if (inputDependencySpecJson == null) {
            inputDependencySpecJson = Map.of();
        }
        if (confirmationPolicyJson == null) {
            confirmationPolicyJson = Map.of();
        }
        if (sortOrder == null) {
            sortOrder = 0;
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now();
        }
    }

    /**
     * Refresh the update timestamp before changing node definition details.
     */
    @PreUpdate
    protected void onUpdate() {
        processingMode = ScheduleNodeProcessingModes.normalize(processingMode);
        updatedAt = LocalDateTime.now();
    }
}
