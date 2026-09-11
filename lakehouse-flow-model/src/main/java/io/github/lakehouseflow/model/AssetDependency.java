package io.github.lakehouseflow.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcType;
import org.hibernate.type.descriptor.jdbc.JsonJdbcType;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Dependency condition for a workflow or task.
 *
 * Defines what a workflow/task is waiting for:
 * - SNAPSHOT_EXISTS: table has a snapshot
 * - WATERMARK_GTE: event time >= specified time
 * - QUALITY_PASSED: quality check must pass
 * - SCHEMA_COMPATIBLE: schema change must be compatible
 *
 * Can be composed with AND/OR operators.
 */
@Entity
@Table(
    name = "asset_dependency",
    indexes = {
        @Index(name = "idx_dep_asset_key", columnList = "asset_key,enabled"),
        @Index(name = "idx_dep_workflow", columnList = "workflow_code,enabled"),
        @Index(name = "idx_dep_task", columnList = "task_code,enabled")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AssetDependency {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Asset key being depended on (e.g., paimon.prod.orders.dt=${biz_date})
     */
    @Column(name = "asset_key", nullable = false, length = 255)
    private String assetKey;

    /**
     * Workflow code this dependency is for (optional, can be null if used by multiple)
     */
    @Column(name = "workflow_code", length = 255)
    private String workflowCode;

    /**
     * Task code this dependency is for (optional)
     */
    @Column(name = "task_code", length = 255)
    private String taskCode;

    /**
     * Dependency conditions as JSON:
     * {
     *   "operator": "AND",
     *   "conditions": [
     *     {"type": "SNAPSHOT_EXISTS"},
     *     {"type": "WATERMARK_GTE", "value": "2025-09-11 23:59:59"},
     *     {"type": "QUALITY_PASSED"},
     *     {"type": "SCHEMA_COMPATIBLE"}
     *   ]
     * }
     */
    @Column(name = "dependency_conditions", nullable = false, columnDefinition = "jsonb")
    @JdbcType(JsonJdbcType.class)
    private Map<String, Object> dependencyConditions;

    /**
     * Is this dependency enabled?
     */
    @Column(name = "enabled", nullable = false)
    private Boolean enabled;

    /**
     * Optimistic lock version
     */
    @Column(name = "version", nullable = false)
    @Version
    private Long version;

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
        if (enabled == null) enabled = true;
        if (version == null) version = 1L;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
