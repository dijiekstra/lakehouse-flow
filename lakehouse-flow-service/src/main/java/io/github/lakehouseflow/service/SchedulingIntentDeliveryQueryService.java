package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Read-only operational queries for scheduling-intent transport evidence.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SchedulingIntentDeliveryQueryService {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 500;
    private static final Set<String> CHANNELS = Set.of(
            SchedulingIntentDeliveryChannels.DATABASE_TABLE,
            SchedulingIntentDeliveryChannels.HTTP,
            SchedulingIntentDeliveryChannels.MQ);

    private final SchedulingIntentDeliveryRepository deliveryRepository;
    private final SchedulingIntentRepository intentRepository;

    /**
     * Find the newest dead-lettered transport records with an optional channel filter.
     *
     * <p>These rows represent exhausted delivery only. They must never be presented as downstream
     * task failures.
     *
     * @param channel optional DATABASE_TABLE, HTTP, or MQ filter
     * @param requestedLimit maximum rows, or null for the default
     * @return newest dead-lettered delivery evidence first
     */
    public List<DeadLetterDelivery> findDeadLetters(String channel, Integer requestedLimit) {
        String normalizedChannel = normalizeChannel(channel);
        int limit = validateLimit(requestedLimit);
        List<SchedulingIntentDelivery> deliveries = normalizedChannel == null
                ? deliveryRepository.findByStatusOrderByDeadLetteredAtDescIdDesc(
                        SchedulingIntentDeliveryStatuses.EXHAUSTED,
                        PageRequest.of(0, limit))
                : deliveryRepository.findByStatusAndChannelOrderByDeadLetteredAtDescIdDesc(
                        SchedulingIntentDeliveryStatuses.EXHAUSTED,
                        normalizedChannel,
                        PageRequest.of(0, limit));
        Map<Long, SchedulingIntent> intents = intentRepository.findAllById(deliveries.stream()
                        .map(SchedulingIntentDelivery::getSchedulingIntentId)
                        .toList())
                .stream()
                .collect(Collectors.toMap(SchedulingIntent::getId, Function.identity()));
        return deliveries.stream()
                .map(delivery -> toDeadLetter(delivery, intents.get(delivery.getSchedulingIntentId())))
                .toList();
    }

    /** Convert one delivery and its immutable intent into an operations read model. */
    private DeadLetterDelivery toDeadLetter(
            SchedulingIntentDelivery delivery,
            SchedulingIntent intent) {
        if (intent == null) {
            throw new IllegalStateException(
                    "Scheduling intent not found for delivery: " + delivery.getId());
        }
        return new DeadLetterDelivery(
                delivery.getId(),
                intent.getId(),
                intent.getIntentKey(),
                intent.getTaskInstanceId(),
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
        String normalized = channel.trim().toUpperCase(java.util.Locale.ROOT);
        if (!CHANNELS.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported scheduling intent delivery channel: " + channel);
        }
        return normalized;
    }

    /** Validate a bounded operations query size. */
    private int validateLimit(Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_LIMIT : requestedLimit;
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("dead-letter query limit must be between 1 and " + MAX_LIMIT);
        }
        return limit;
    }

    /**
     * Read model for one exhausted delivery route.
     *
     * @param deliveryId transport delivery id
     * @param intentId immutable scheduling intent id
     * @param intentKey downstream idempotency and snapshot attribution key
     * @param taskInstanceId task scheduling instance id
     * @param channel selected transport channel
     * @param destination selected endpoint, topic, or table
     * @param attemptCount infrastructure publication attempts
     * @param lastError latest transport error
     * @param lastAttemptAt latest infrastructure attempt time
     * @param deliverBefore publication admission deadline
     * @param deadLetteredAt time the route entered EXHAUSTED
     */
    public record DeadLetterDelivery(
            Long deliveryId,
            Long intentId,
            String intentKey,
            Long taskInstanceId,
            String channel,
            String destination,
            Integer attemptCount,
            String lastError,
            LocalDateTime lastAttemptAt,
            LocalDateTime deliverBefore,
            LocalDateTime deadLetteredAt) {
    }
}
