package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.JobControlIntentContract;
import io.github.lakehouseflow.common.JobControlOperations;
import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.JobControlIntentDelivery;
import io.github.lakehouseflow.model.WriterJobBinding;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Creates independent platform start and restart intents for streaming writers.
 *
 * No workflow or task instance is created. The transaction only allocates a
 * fenced writer generation and commits its immutable delivery instruction.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class JobControlIntentService {

    private static final String DATABASE_OUTBOX_DESTINATION = "job_control_intent";
    private static final Set<String> SUPPORTED_DELIVERY_CHANNELS = Set.of(
            SchedulingIntentDeliveryChannels.DATABASE_TABLE,
            SchedulingIntentDeliveryChannels.HTTP,
            SchedulingIntentDeliveryChannels.MQ);

    private final WriterJobBindingService writerJobBindingService;
    private final SnapshotProgressService snapshotProgressService;
    private final JobControlIntentRepository jobControlIntentRepository;
    private final JobControlIntentDeliveryRepository jobControlIntentDeliveryRepository;

    /** Maximum interval in which a transport may activate this platform operation. */
    @Value("${lakehouse-flow.job-control-intent-delivery.deliver-window:PT10M}")
    private Duration deliverWindow = Duration.ofMinutes(10);

    /** Window in which attributable data may confirm the new writer generation. */
    @Value("${lakehouse-flow.job-control-intent-delivery.confirmation-timeout:PT10M}")
    private Duration confirmationTimeout = Duration.ofMinutes(10);

    /** Selected deployment route for every job-control intent. */
    @Value("${lakehouse-flow.job-control-intent-delivery.channel:DATABASE_TABLE}")
    private String deliveryChannel = SchedulingIntentDeliveryChannels.DATABASE_TABLE;

    /** Channel-specific destination for job-control publication. */
    @Value("${lakehouse-flow.job-control-intent-delivery.destination:job_control_intent}")
    private String deliveryDestination = DATABASE_OUTBOX_DESTINATION;

    /**
     * Start a writer that has not yet received any generation.
     *
     * @param writerJobKey stable platform writer key
     * @param command idempotent operator request
     * @return immutable control intent and delivery evidence
     */
    public JobControlIntentView startJob(String writerJobKey, CreateJobControlIntentCommand command) {
        return createControlIntent(writerJobKey, JobControlOperations.START_JOB, command);
    }

    /**
     * Restart a writer by fencing its current generation and allocating the next one.
     *
     * @param writerJobKey stable platform writer key
     * @param command idempotent operator request
     * @return immutable control intent and delivery evidence
     */
    public JobControlIntentView restartJob(String writerJobKey, CreateJobControlIntentCommand command) {
        return createControlIntent(writerJobKey, JobControlOperations.RESTART_JOB, command);
    }

    /**
     * Find one control intent by its epoch-derived key.
     *
     * @param intentKey immutable job-control intent key
     * @return control and delivery audit when present
     */
    @Transactional(readOnly = true)
    public Optional<JobControlIntentView> findIntent(String intentKey) {
        if (intentKey == null || intentKey.isBlank()) {
            return Optional.empty();
        }
        return jobControlIntentRepository.findByIntentKey(intentKey.trim()).map(this::toView);
    }

    /** Create one idempotent control instruction under the binding's epoch lock. */
    private JobControlIntentView createControlIntent(
            String writerJobKey,
            String operationType,
            CreateJobControlIntentCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("job control command is required");
        }
        String requestKey = requireText(command.requestKey(), "requestKey");
        Optional<JobControlIntent> existing = jobControlIntentRepository.findByRequestKey(requestKey);
        if (existing.isPresent()) {
            requireSameRequest(existing.get(), writerJobKey, operationType);
            return toView(existing.get());
        }
        Duration normalizedDeliverWindow = requirePositiveDuration(deliverWindow, "deliverWindow");
        Duration normalizedConfirmationTimeout = requirePositiveDuration(
                confirmationTimeout,
                "confirmationTimeout");
        String selectedChannel = requireDeliveryChannel();
        String selectedDestination = requireDeliveryDestination();
        WriterJobBindingService.ControlEpochAllocation allocation =
                writerJobBindingService.allocateControlEpoch(writerJobKey, operationType);
        WriterJobBinding binding = allocation.binding();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime deliverBefore = now.plus(normalizedDeliverWindow);
        LocalDateTime confirmationDeadline = now.plus(normalizedConfirmationTimeout);
        String baselineSnapshotId = snapshotProgressService.findLatestSnapshotId(binding.getTableAssetKey())
                .orElse(null);
        String requestedBy = requireText(command.requestedBy(), "requestedBy");
        Map<String, Object> payload = buildPayload(
                allocation,
                operationType,
                requestedBy,
                blankToNull(command.reason()),
                baselineSnapshotId,
                deliverBefore,
                normalizedConfirmationTimeout,
                now);
        JobControlIntent intent = jobControlIntentRepository.save(JobControlIntent.builder()
                .contractVersion(JobControlIntentContract.CONTRACT_VERSION)
                .requestKey(requestKey)
                .intentKey(allocation.intentKey())
                .writerJobBindingId(binding.getId())
                .writerJobKey(binding.getWriterJobKey())
                .tableAssetKey(binding.getTableAssetKey())
                .operationType(operationType)
                .processingMode(ScheduleNodeProcessingModes.STREAMING)
                .writerEpoch(allocation.writerEpoch())
                .previousWriterEpoch(allocation.previousWriterEpoch())
                .baselineSnapshotId(baselineSnapshotId)
                .deliverBefore(deliverBefore)
                .confirmationDeadline(confirmationDeadline)
                .requestedBy(requestedBy)
                .reason(blankToNull(command.reason()))
                .snapshotResult(JobControlIntentContract.WAITING)
                .instructionPayloadJson(payload)
                .createdAt(now)
                .build());
        boolean databaseOutbox = SchedulingIntentDeliveryChannels.DATABASE_TABLE.equals(selectedChannel);
        JobControlIntentDelivery delivery = jobControlIntentDeliveryRepository.save(
                JobControlIntentDelivery.builder()
                        .jobControlIntentId(intent.getId())
                        .channel(selectedChannel)
                        .destination(selectedDestination)
                        .status(databaseOutbox
                                ? SchedulingIntentDeliveryStatuses.PUBLISHED
                                : SchedulingIntentDeliveryStatuses.PENDING)
                        .attemptCount(databaseOutbox ? 1 : 0)
                        .lastAttemptAt(databaseOutbox ? now : null)
                        .deliverBefore(deliverBefore)
                        .publishedAt(databaseOutbox ? now : null)
                        .createdAt(now)
                        .updatedAt(now)
                        .build());
        return toView(intent, delivery);
    }

    /** Build the complete channel-neutral job-control payload. */
    private Map<String, Object> buildPayload(
            WriterJobBindingService.ControlEpochAllocation allocation,
            String operationType,
            String requestedBy,
            String reason,
            String baselineSnapshotId,
            LocalDateTime deliverBefore,
            Duration effectiveConfirmationTimeout,
            LocalDateTime issuedAt) {
        WriterJobBinding binding = allocation.binding();
        Map<String, Object> writer = new LinkedHashMap<>();
        writer.put("writerJobKey", binding.getWriterJobKey());
        writer.put("tableAssetKey", binding.getTableAssetKey());
        writer.put("writerEpoch", allocation.writerEpoch());
        writer.put("previousWriterEpoch", allocation.previousWriterEpoch());
        writer.put("processingMode", ScheduleNodeProcessingModes.STREAMING);

        Map<String, Object> control = new LinkedHashMap<>();
        control.put("operationType", operationType);
        control.put("reason", reason);
        control.put("requestedBy", requestedBy);
        control.put("deliverBefore", deliverBefore.toString());

        Map<String, String> requiredProperties = new LinkedHashMap<>();
        requiredProperties.put(
                SnapshotEvidenceContract.SOURCE_PROPERTY,
                SnapshotEvidenceContract.INTENT_SOURCE);
        requiredProperties.put(
                SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY,
                allocation.intentKey());
        requiredProperties.put(
                SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY,
                binding.getWriterJobKey());
        requiredProperties.put(
                SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY,
                Long.toString(allocation.writerEpoch()));

        Map<String, Object> snapshotObservation = new LinkedHashMap<>();
        snapshotObservation.put("targetTableAssetKey", binding.getTableAssetKey());
        snapshotObservation.put("baselineSnapshotId", baselineSnapshotId);
        snapshotObservation.put("confirmationTimeout", effectiveConfirmationTimeout.toString());
        snapshotObservation.put("requiredWriterProperties", requiredProperties);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", JobControlIntentContract.CONTRACT_VERSION);
        payload.put("source", SnapshotEvidenceContract.INTENT_SOURCE);
        payload.put("intentKind", "JOB_CONTROL");
        payload.put("intentKey", allocation.intentKey());
        payload.put("issuedAt", issuedAt.toString());
        payload.put("writer", writer);
        payload.put("control", control);
        payload.put("snapshotObservation", snapshotObservation);
        return payload;
    }

    /** Require an idempotent replay to describe the same writer and operation. */
    private void requireSameRequest(JobControlIntent existing, String writerJobKey, String operationType) {
        if (!existing.getWriterJobKey().equals(requireText(writerJobKey, "writerJobKey"))
                || !existing.getOperationType().equals(JobControlOperations.normalize(operationType))) {
            throw new IllegalStateException(
                    "Job control request key is already used by a different operation: " + existing.getRequestKey());
        }
    }

    /** Convert a persisted control intent and delivery into an audit view. */
    private JobControlIntentView toView(JobControlIntent intent, JobControlIntentDelivery delivery) {
        return new JobControlIntentView(
                intent.getId(),
                intent.getContractVersion(),
                intent.getRequestKey(),
                intent.getIntentKey(),
                intent.getWriterJobKey(),
                intent.getTableAssetKey(),
                intent.getOperationType(),
                intent.getProcessingMode(),
                intent.getWriterEpoch(),
                intent.getPreviousWriterEpoch(),
                intent.getBaselineSnapshotId(),
                intent.getDeliverBefore(),
                intent.getConfirmationDeadline(),
                intent.getRequestedBy(),
                intent.getReason(),
                intent.getSnapshotResult(),
                intent.getObservedSnapshotId(),
                intent.getWaitingReason(),
                intent.getSourceHealth(),
                intent.getSourceHealthDetail(),
                intent.getSourceEvidenceCheckedAt(),
                intent.getLastSnapshotCheckAt(),
                intent.getInstructionPayloadJson(),
                delivery.getChannel(),
                delivery.getDestination(),
                delivery.getStatus(),
                delivery.getAttemptCount(),
                delivery.getLastError(),
                delivery.getLastAttemptAt(),
                delivery.getNextAttemptAt(),
                delivery.getDeadLetteredAt(),
                delivery.getPublishedAt(),
                intent.getCreatedAt());
    }

    /** Load the selected delivery before constructing an audit view. */
    private JobControlIntentView toView(JobControlIntent intent) {
        JobControlIntentDelivery delivery = jobControlIntentDeliveryRepository
                .findByJobControlIntentId(intent.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Job control delivery evidence not found: " + intent.getId()));
        return toView(intent, delivery);
    }

    /** Normalize and validate the selected delivery channel. */
    private String requireDeliveryChannel() {
        String selected = requireText(deliveryChannel, "job control delivery channel")
                .toUpperCase(java.util.Locale.ROOT);
        if (!SUPPORTED_DELIVERY_CHANNELS.contains(selected)) {
            throw new IllegalStateException("Unsupported job control delivery channel: " + deliveryChannel);
        }
        return selected;
    }

    /** Require one nonblank route destination. */
    private String requireDeliveryDestination() {
        return requireText(deliveryDestination, "job control delivery destination");
    }

    /** Require one positive configuration duration. */
    private Duration requirePositiveDuration(Duration value, String fieldName) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalStateException(fieldName + " must be positive");
        }
        return value;
    }

    /** Require and trim one textual request field. */
    private String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /** Convert optional blank audit text to null. */
    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    /**
     * Idempotent operator request for one writer lifecycle operation.
     *
     * @param requestKey caller-supplied unique operation key
     * @param requestedBy operator or system audit identity
     * @param reason optional operation reason
     */
    public record CreateJobControlIntentCommand(
            String requestKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Read model keeping snapshot and delivery evidence orthogonal.
     *
     * @param intentId immutable control intent id
     * @param contractVersion payload contract version
     * @param requestKey caller idempotency key
     * @param intentKey epoch-derived downstream idempotency key
     * @param writerJobKey stable writer routing key
     * @param tableAssetKey normalized physical table
     * @param operationType START_JOB or RESTART_JOB
     * @param processingMode engine-neutral writer mode
     * @param writerEpoch newly allocated generation
     * @param previousWriterEpoch generation to fence
     * @param baselineSnapshotId snapshot observed before the operation
     * @param deliverBefore external admission deadline
     * @param confirmationDeadline end of the snapshot observation window
     * @param requestedBy audit identity
     * @param reason optional operator reason
     * @param snapshotResult independent snapshot outcome
     * @param observedSnapshotId attributable snapshot when present
     * @param waitingReason current snapshot wait explanation
     * @param sourceHealth independent source-health outcome
     * @param sourceHealthDetail source reconciliation detail
     * @param sourceEvidenceCheckedAt source evidence timestamp
     * @param lastSnapshotCheckAt latest snapshot evaluation time
     * @param instructionPayload complete immutable outbound instruction
     * @param deliveryChannel selected transport
     * @param deliveryDestination selected destination
     * @param deliveryStatus transport-only state
     * @param deliveryAttemptCount number of publication attempts
     * @param deliveryLastError latest transport error
     * @param deliveryLastAttemptAt latest transport attempt time
     * @param deliveryNextAttemptAt next retry time
     * @param deliveryDeadLetteredAt transport dead-letter time
     * @param publishedAt transport acknowledgement time
     * @param createdAt immutable creation time
     */
    public record JobControlIntentView(
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
}
