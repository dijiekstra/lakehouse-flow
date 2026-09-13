package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.FlowPlanVersionStatuses;
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
import java.util.Map;

/**
 * Versioned scheduler definition for one FlowPlan.
 *
 * A published version is the durable anchor for emitted scheduling instances and
 * action records. JSON columns keep policy details flexible while the first
 * class columns keep identity, versioning, and publication state queryable.
 */
@Entity
@Table(
    name = "flow_plan_version",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_flow_plan_version", columnNames = {"flow_plan_id", "version"})
    },
    indexes = {
        @Index(name = "idx_flow_plan_version_plan", columnList = "flow_plan_id,version DESC"),
        @Index(name = "idx_flow_plan_version_code", columnList = "flow_code,version DESC"),
        @Index(name = "idx_flow_plan_version_status", columnList = "status,updated_at DESC")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FlowPlanVersion {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Owning FlowPlan id.
     */
    @Column(name = "flow_plan_id", nullable = false)
    private Long flowPlanId;

    /**
     * Denormalized Flow code for audit and lookup convenience.
     */
    @Column(name = "flow_code", nullable = false, length = 255)
    private String flowCode;

    /**
     * Monotonically increasing version number within the FlowPlan.
     */
    @Column(name = "version", nullable = false)
    private Integer version;

    /**
     * Version lifecycle status.
     */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /**
     * Graph-level metadata and visualization hints.
     */
    @Column(name = "graph_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> graphJson;

    /**
     * Flow-level dependency defaults inherited by nodes when applicable.
     */
    @Column(name = "dependency_spec_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> dependencySpecJson;

    /**
     * Trigger policy such as schedule windows or snapshot trigger rules.
     */
    @Column(name = "trigger_policy_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> triggerPolicyJson;

    /**
     * Snapshot confirmation policy shared by nodes unless overridden.
     */
    @Column(name = "confirmation_policy_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> confirmationPolicyJson;

    /**
     * Scheduler-side concurrency policy for this Flow version.
     */
    @Column(name = "concurrency_policy_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> concurrencyPolicyJson;

    /**
     * User or system identity that published this version.
     */
    @Column(name = "published_by", length = 255)
    private String publishedBy;

    /**
     * Time when this version was published.
     */
    @Column(name = "published_at")
    private LocalDateTime publishedAt;

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
     * Mark this version as published.
     *
     * @param publisher user or system identity that publishes this version
     */
    public void markPublished(String publisher) {
        status = FlowPlanVersionStatuses.PUBLISHED;
        publishedBy = publisher;
        publishedAt = LocalDateTime.now();
    }

    /**
     * Initialize default lifecycle, JSON policy maps, and audit timestamps before insert.
     */
    @PrePersist
    protected void onCreate() {
        if (status == null) {
            status = FlowPlanVersionStatuses.DRAFT;
        }
        if (graphJson == null) {
            graphJson = Map.of();
        }
        if (dependencySpecJson == null) {
            dependencySpecJson = Map.of();
        }
        if (triggerPolicyJson == null) {
            triggerPolicyJson = Map.of();
        }
        if (confirmationPolicyJson == null) {
            confirmationPolicyJson = Map.of();
        }
        if (concurrencyPolicyJson == null) {
            concurrencyPolicyJson = Map.of();
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now();
        }
    }

    /**
     * Refresh the update timestamp before changing version definition state.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
