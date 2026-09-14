package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.JobControlIntentContract;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
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
 * Immutable platform instruction for starting or restarting one writer generation.
 *
 * This object is deliberately independent from workflow, task, business-date, and
 * execution status models. Its outcome is inferred only from attributable snapshots.
 */
@Entity
@Table(
        name = "job_control_intent",
        uniqueConstraints = {
            @UniqueConstraint(name = "uk_job_control_intent_key", columnNames = "intent_key"),
            @UniqueConstraint(name = "uk_job_control_request_key", columnNames = "request_key"),
            @UniqueConstraint(
                    name = "uk_job_control_writer_epoch",
                    columnNames = {"writer_job_binding_id", "writer_epoch"})
        },
        indexes = {
            @Index(name = "idx_job_control_writer", columnList = "writer_job_key,created_at DESC"),
            @Index(name = "idx_job_control_snapshot", columnList = "snapshot_result,confirmation_deadline ASC")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobControlIntent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Job-control contract version frozen into the payload. */
    @Column(name = "contract_version", nullable = false, updatable = false, length = 32)
    private String contractVersion;

    /** Caller-supplied idempotency key for one platform operation request. */
    @Column(name = "request_key", nullable = false, unique = true, updatable = false, length = 255)
    private String requestKey;

    /** Epoch-derived downstream idempotency and snapshot-attribution key. */
    @Column(name = "intent_key", nullable = false, unique = true, updatable = false, length = 255)
    private String intentKey;

    /** Binding locked while this generation was allocated. */
    @Column(name = "writer_job_binding_id", nullable = false, updatable = false)
    private Long writerJobBindingId;

    /** Stable platform routing key copied from the binding. */
    @Column(name = "writer_job_key", nullable = false, updatable = false, length = 255)
    private String writerJobKey;

    /** Normalized physical table owned by the writer. */
    @Column(name = "table_asset_key", nullable = false, updatable = false, length = 255)
    private String tableAssetKey;

    /** START_JOB or RESTART_JOB platform operation. */
    @Column(name = "operation_type", nullable = false, updatable = false, length = 32)
    private String operationType;

    /** Engine-neutral processing mode authorized by this control operation. */
    @Column(name = "processing_mode", nullable = false, updatable = false, length = 32)
    private String processingMode;

    /** Newly allocated fencing generation. */
    @Column(name = "writer_epoch", nullable = false, updatable = false)
    private Long writerEpoch;

    /** Writer generation that must be fenced before this one activates. */
    @Column(name = "previous_writer_epoch", nullable = false, updatable = false)
    private Long previousWriterEpoch;

    /** Latest physical table snapshot observed before issuing the operation. */
    @Column(name = "baseline_snapshot_id", updatable = false, length = 255)
    private String baselineSnapshotId;

    /** Latest time an external transport may start this operation. */
    @Column(name = "deliver_before", nullable = false, updatable = false)
    private LocalDateTime deliverBefore;

    /** End of the snapshot observation window for this writer generation. */
    @Column(name = "confirmation_deadline", nullable = false, updatable = false)
    private LocalDateTime confirmationDeadline;

    /** Operator or system identity recorded for audit only. */
    @Column(name = "requested_by", nullable = false, updatable = false, length = 255)
    private String requestedBy;

    /** Human-readable platform operation reason. */
    @Column(name = "reason", columnDefinition = "text", updatable = false)
    private String reason;

    /** WAITING, SNAPSHOT_CONFIRMED, or SNAPSHOT_NOT_ADVANCED evidence result. */
    @Builder.Default
    @Column(name = "snapshot_result", nullable = false, length = 32)
    private String snapshotResult = JobControlIntentContract.WAITING;

    /** Attributable snapshot observed for the selected writer generation. */
    @Column(name = "observed_snapshot_id", length = 255)
    private String observedSnapshotId;

    /** Current snapshot wait or independent source-blocking explanation. */
    @Column(name = "waiting_reason", columnDefinition = "text")
    private String waitingReason;

    /** Independent source-health outcome used when the confirmation window ends. */
    @Column(name = "source_health", length = 32)
    private String sourceHealth;

    /** Source reconciliation detail supporting the current conclusion. */
    @Column(name = "source_health_detail", columnDefinition = "text")
    private String sourceHealthDetail;

    /** Time at which the supporting source evidence was checked. */
    @Column(name = "source_evidence_checked_at")
    private LocalDateTime sourceEvidenceCheckedAt;

    /** Last time Lakehouse Flow evaluated target snapshot evidence. */
    @Column(name = "last_snapshot_check_at")
    private LocalDateTime lastSnapshotCheckAt;

    /** Complete channel-neutral job-control payload. */
    @Column(name = "instruction_payload_json", nullable = false, updatable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> instructionPayloadJson;

    /** Immutable intent creation timestamp. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /** Initialize snapshot evidence defaults and creation time before insert. */
    @PrePersist
    protected void onCreate() {
        if (snapshotResult == null) {
            snapshotResult = JobControlIntentContract.WAITING;
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }
}
