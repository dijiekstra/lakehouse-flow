package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * API response keeping writer snapshot outcome and transport state independent.
 *
 * @param intentId control intent id
 * @param contractVersion job-control payload version
 * @param requestKey caller idempotency key
 * @param intentKey writer epoch idempotency key
 * @param writerJobKey stable writer key
 * @param tableAssetKey normalized physical table
 * @param operationType START_JOB or RESTART_JOB
 * @param processingMode engine-neutral writer mode
 * @param writerEpoch new fenced generation
 * @param previousWriterEpoch previous generation
 * @param baselineSnapshotId pre-operation snapshot
 * @param deliverBefore external operation deadline
 * @param confirmationDeadline snapshot observation deadline
 * @param requestedBy audit identity
 * @param reason operation reason
 * @param snapshotResult snapshot-only outcome
 * @param observedSnapshotId attributable snapshot when present
 * @param waitingReason current wait reason
 * @param sourceHealth independent source health
 * @param sourceHealthDetail source reconciliation detail
 * @param sourceEvidenceCheckedAt source evidence timestamp
 * @param lastSnapshotCheckAt latest snapshot check timestamp
 * @param instructionPayload immutable outbound payload
 * @param deliveryChannel selected transport
 * @param deliveryDestination selected destination
 * @param deliveryStatus transport-only state
 * @param deliveryAttemptCount publication attempts
 * @param deliveryLastError latest transport error
 * @param deliveryLastAttemptAt latest publication attempt
 * @param deliveryNextAttemptAt next retry time
 * @param deliveryDeadLetteredAt dead-letter time
 * @param publishedAt transport acknowledgement time
 * @param createdAt intent creation time
 */
public record JobControlIntentResponse(
        Long intentId,
        String contractVersion,
        String requestKey,
        String intentKey,
        String writerJobKey,
        String tableAssetKey,
        String operationType,
        String processingMode,
        Long writerEpoch,
        Long previousWriterEpoch,
        String baselineSnapshotId,
        LocalDateTime deliverBefore,
        LocalDateTime confirmationDeadline,
        String requestedBy,
        String reason,
        String snapshotResult,
        String observedSnapshotId,
        String waitingReason,
        String sourceHealth,
        String sourceHealthDetail,
        LocalDateTime sourceEvidenceCheckedAt,
        LocalDateTime lastSnapshotCheckAt,
        Map<String, Object> instructionPayload,
        String deliveryChannel,
        String deliveryDestination,
        String deliveryStatus,
        Integer deliveryAttemptCount,
        String deliveryLastError,
        LocalDateTime deliveryLastAttemptAt,
        LocalDateTime deliveryNextAttemptAt,
        LocalDateTime deliveryDeadLetteredAt,
        LocalDateTime publishedAt,
        LocalDateTime createdAt) {
}
