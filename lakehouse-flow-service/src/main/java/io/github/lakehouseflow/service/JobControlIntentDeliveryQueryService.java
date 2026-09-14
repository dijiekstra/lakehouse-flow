package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.AssetKeys;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.JobControlIntentDelivery;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Read-only operations queries for job-control intent transport evidence.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class JobControlIntentDeliveryQueryService {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 500;
    private static final Set<String> CHANNELS = Set.of(
            SchedulingIntentDeliveryChannels.DATABASE_TABLE,
            SchedulingIntentDeliveryChannels.HTTP,
            SchedulingIntentDeliveryChannels.MQ);

    private final JobControlIntentDeliveryRepository deliveryRepository;
    private final JobControlIntentRepository intentRepository;

    /**
     * Find newest exhausted job-control delivery routes with an optional channel filter.
     *
     * @param channel optional DATABASE_TABLE, HTTP, or MQ filter
     * @param requestedLimit maximum rows, or null for the default
     * @return newest exhausted transport evidence first
     */
    public List<DeadLetterDelivery> findDeadLetters(String channel, Integer requestedLimit) {
        return findDeadLetters(channel, null, null, requestedLimit);
    }

    /**
     * Find newest exhausted job-control routes within an optional Flow and target scope.
     *
     * @param channel optional DATABASE_TABLE, HTTP, or MQ filter
     * @param flowCode optional Flow whose node output uses the writer table
     * @param targetAssetKey optional table or partition target
     * @param requestedLimit maximum rows, or null for the default
     * @return newest matching exhausted transport evidence first
     */
    public List<DeadLetterDelivery> findDeadLetters(
            String channel,
            String flowCode,
            String targetAssetKey,
            Integer requestedLimit) {
        String normalizedChannel = normalizeChannel(channel);
        int limit = validateLimit(requestedLimit);
        List<JobControlIntentDelivery> deliveries = deliveryRepository.findDeadLetters(
                        SchedulingIntentDeliveryStatuses.EXHAUSTED,
                        normalizedChannel,
                        normalizeText(flowCode),
                        normalizeTableAssetKey(targetAssetKey),
                        PageRequest.of(0, limit));
        Map<Long, JobControlIntent> intents = intentRepository.findAllById(deliveries.stream()
                        .map(JobControlIntentDelivery::getJobControlIntentId)
                        .toList())
                .stream()
                .collect(Collectors.toMap(JobControlIntent::getId, Function.identity()));
        return deliveries.stream()
                .map(delivery -> toDeadLetter(delivery, intents.get(delivery.getJobControlIntentId())))
                .toList();
    }

    /** Convert one delivery and its immutable control instruction into an operations view. */
    private DeadLetterDelivery toDeadLetter(
            JobControlIntentDelivery delivery,
            JobControlIntent intent) {
        if (intent == null) {
            throw new IllegalStateException(
                    "Job control intent not found for delivery: " + delivery.getId());
        }
        return new DeadLetterDelivery(
                delivery.getId(),
                intent.getId(),
                intent.getIntentKey(),
                intent.getWriterJobKey(),
                intent.getTableAssetKey(),
                intent.getOperationType(),
                intent.getWriterEpoch(),
                delivery.getChannel(),
                delivery.getDestination(),
                delivery.getAttemptCount(),
                delivery.getLastError(),
                delivery.getLastAttemptAt(),
                delivery.getDeliverBefore(),
                delivery.getDeadLetteredAt());
    }

    /** Normalize and validate an optional transport channel filter. */
    private String normalizeChannel(String channel) {
        if (channel == null || channel.isBlank()) {
            return null;
        }
        String normalized = channel.trim().toUpperCase(Locale.ROOT);
        if (!CHANNELS.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported job control delivery channel: " + channel);
        }
        return normalized;
    }

    /** Normalize a table or partition key to the managed physical table identity. */
    private String normalizeTableAssetKey(String targetAssetKey) {
        if (targetAssetKey == null || targetAssetKey.isBlank()) {
            return null;
        }
        return AssetKeys.tableKey(targetAssetKey.trim())
                .orElseThrow(() -> new IllegalArgumentException(
                        "targetAssetKey must be catalog.database.table[.partition]"));
    }

    /** Normalize optional scope text by trimming it. */
    private String normalizeText(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    /** Validate a bounded operations query size. */
    private int validateLimit(Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_LIMIT : requestedLimit;
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("job-control dead-letter query limit must be between 1 and " + MAX_LIMIT);
        }
        return limit;
    }

    /**
     * Read model for one exhausted writer lifecycle delivery.
     *
     * @param deliveryId transport delivery id
     * @param intentId immutable job-control intent id
     * @param intentKey writer epoch idempotency and snapshot attribution key
     * @param writerJobKey stable platform writer key
     * @param tableAssetKey writer-owned physical table
     * @param operationType START_JOB or RESTART_JOB
     * @param writerEpoch selected fenced writer generation
     * @param channel selected transport channel
     * @param destination selected endpoint, topic, or table
     * @param attemptCount infrastructure publication attempts
     * @param lastError latest transport error
     * @param lastAttemptAt latest infrastructure attempt time
     * @param deliverBefore operation admission deadline
     * @param deadLetteredAt time the route entered EXHAUSTED
     */
    public record DeadLetterDelivery(
            Long deliveryId,
            Long intentId,
            String intentKey,
            String writerJobKey,
            String tableAssetKey,
            String operationType,
            Long writerEpoch,
            String channel,
            String destination,
            Integer attemptCount,
            String lastError,
            LocalDateTime lastAttemptAt,
            LocalDateTime deliverBefore,
            LocalDateTime deadLetteredAt) {
    }
}
