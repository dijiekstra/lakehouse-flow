package io.github.lakehouseflow.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * One execution of a task.
 *
 * State machine:
 * CREATED → WAITING_DEPENDENCY → READY → DISPATCHING → RUNNING → SUCCESS
 *                                                              ↘ FAILED
 *                                                              ↘ TIMEOUT
 * FAILED → RETRY_WAITING → READY (when retries remain)
 *
 * Unique key: workflow_instance_id + task_code
 */
@Entity
@Table(
    name = "task_instance",
    indexes = {
        @Index(name = "idx_task_instance_key", columnList = "instance_key", unique = true),
        @Index(name = "idx_workflow_task", columnList = "workflow_instance_id,task_code"),
        @Index(name = "idx_task_state", columnList = "state,updated_at DESC"),
        @Index(name = "idx_external_job", columnList = "external_job_id")
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
     * Unique instance key within workflow instance
     * Format: workflow_instance_id:task_code:try_number
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
     * Business date
     */
    @Column(name = "biz_date", nullable = false)
    private LocalDateTime bizDate;

    /**
     * Task state: CREATED, WAITING_DEPENDENCY, READY, DISPATCHING, RUNNING, SUCCESS, FAILED, RETRY_WAITING, TIMEOUT, CANCELLED, SKIPPED
     */
    @Column(name = "state", nullable = false, length = 50)
    private String state;

    /**
     * Reason why task is in WAITING_DEPENDENCY state
     * Example: "Waiting for: paimon.prod.orders.dt=2025-09-11 (no snapshot yet)"
     */
    @Column(name = "waiting_reason", columnDefinition = "text")
    private String waitingReason;

    /**
     * Current attempt number (1-based)
     */
    @Column(name = "try_number", nullable = false)
    private Integer tryNumber;

    /**
     * Maximum retries allowed
     */
    @Column(name = "max_retries", nullable = false)
    private Integer maxRetries;

    /**
     * Executor type: SHELL, HTTP, SQL, SPARK, FLINK, etc.
     */
    @Column(name = "executor_type", length = 50)
    private String executorType;

    /**
     * External job ID (from executor system)
     * Example: Spark application ID, K8s job name, etc.
     */
    @Column(name = "external_job_id", length = 255)
    private String externalJobId;

    /**
     * When task was submitted to executor
     */
    @Column(name = "submit_time")
    private LocalDateTime submitTime;

    /**
     * When task started executing
     */
    @Column(name = "start_time")
    private LocalDateTime startTime;

    /**
     * When task finished
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
        if (tryNumber == null) tryNumber = 1;
        if (maxRetries == null) maxRetries = 3;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
