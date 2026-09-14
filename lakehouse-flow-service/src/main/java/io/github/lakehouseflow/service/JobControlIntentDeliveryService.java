package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.JobControlIntentDelivery;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applies the shared reliable delivery algorithm to independent job-control intents.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class JobControlIntentDeliveryService {

    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int MAX_BATCH_SIZE = 500;
    private static final Set<String> WAITING_STATUSES = Set.of(
            SchedulingIntentDeliveryStatuses.PENDING,
            SchedulingIntentDeliveryStatuses.RETRY_WAIT);

    private final JobControlIntentDeliveryRepository deliveryRepository;
    private final JobControlIntentRepository intentRepository;

    /**
     * Claim due job-control publications for one scheduler process.
     *
     * @param claimOwner stable scheduler process identifier
     * @param requestedLimit maximum claims, or null for the default
     * @param claimLease maximum time the current process owns a claim
     * @return immutable publications safe to push outside the transaction
     */
    public List<SchedulingIntentPublication> claimDueDeliveries(
            String claimOwner,
            Integer requestedLimit,
            Duration claimLease) {
        String normalizedOwner = requireText(claimOwner, "claimOwner");
        int limit = validateBatchSize(requestedLimit);
        Duration normalizedLease = requirePositiveDuration(claimLease, "claimLease");
        LocalDateTime now = LocalDateTime.now();
        return deliveryRepository.findClaimableForUpdate(
                        WAITING_STATUSES,
                        SchedulingIntentDeliveryStatuses.PUBLISHING,
                        now,
                        PageRequest.of(0, limit))
                .stream()
                .map(delivery -> claim(delivery, normalizedOwner, normalizedLease, now))
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    /**
     * Record a transport acknowledgement for the current fenced claim.
     *
     * @param deliveryId claimed delivery id
     * @param claimToken current fencing token
     * @return true when acknowledged, false for a stale claim
     */
    public boolean recordPublished(Long deliveryId, String claimToken) {
        JobControlIntentDelivery delivery = lockDelivery(deliveryId);
        if (SchedulingIntentDeliveryStatuses.PUBLISHED.equals(delivery.getStatus())) {
            return true;
        }
        boolean recorded = IntentDeliveryReliability.recordPublished(
                delivery,
                claimToken,
                LocalDateTime.now());
        if (recorded) {
            deliveryRepository.save(delivery);
        }
        return recorded;
    }

    /**
     * Record a failed transport and schedule retry or terminal exhaustion.
     *
     * @param deliveryId claimed delivery id
     * @param claimToken current fencing token
     * @param error transport-only failure description
     * @param maxAttempts maximum publication attempts
     * @param initialBackoff first retry delay
     * @param maximumBackoff bounded retry delay
     * @return fenced failure outcome
     */
    public SchedulingIntentDeliveryFailureResult recordFailure(
            Long deliveryId,
            String claimToken,
            String error,
            int maxAttempts,
            Duration initialBackoff,
            Duration maximumBackoff) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        Duration normalizedInitial = requirePositiveDuration(initialBackoff, "initialBackoff");
        Duration normalizedMaximum = requirePositiveDuration(maximumBackoff, "maximumBackoff");
        if (normalizedMaximum.compareTo(normalizedInitial) < 0) {
            throw new IllegalArgumentException("maximumBackoff must be greater than or equal to initialBackoff");
        }
        JobControlIntentDelivery delivery = lockDelivery(deliveryId);
        SchedulingIntentDeliveryFailureResult result = IntentDeliveryReliability.recordFailure(
                delivery,
                claimToken,
                error,
                maxAttempts,
                normalizedInitial,
                normalizedMaximum,
                LocalDateTime.now());
        if (result != SchedulingIntentDeliveryFailureResult.STALE) {
            deliveryRepository.save(delivery);
        }
        return result;
    }

    /** Claim one candidate or persist transport exhaustion after its deadline. */
    private SchedulingIntentPublication claim(
            JobControlIntentDelivery delivery,
            String claimOwner,
            Duration claimLease,
            LocalDateTime now) {
        java.util.Optional<String> claimToken = IntentDeliveryReliability.claim(
                delivery,
                claimOwner,
                claimLease,
                now);
        deliveryRepository.save(delivery);
        if (claimToken.isEmpty()) {
            return null;
        }
        JobControlIntent intent = intentRepository.findById(delivery.getJobControlIntentId())
                .orElseThrow(() -> new IllegalStateException(
                        "Job control intent not found for delivery: " + delivery.getId()));
        Map<String, Object> payload = intent.getInstructionPayloadJson() == null
                ? Map.of()
                : Map.copyOf(new LinkedHashMap<>(intent.getInstructionPayloadJson()));
        return new SchedulingIntentPublication(
                delivery.getId(),
                claimToken.get(),
                delivery.getChannel(),
                delivery.getDestination(),
                intent.getId(),
                intent.getIntentKey(),
                intent.getContractVersion(),
                payload);
    }

    /** Lock one job-control delivery and reject invalid identifiers. */
    private JobControlIntentDelivery lockDelivery(Long deliveryId) {
        if (deliveryId == null || deliveryId <= 0) {
            throw new IllegalArgumentException("deliveryId must be positive");
        }
        return deliveryRepository.findByIdForUpdate(deliveryId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Job control intent delivery not found: " + deliveryId));
    }

    /** Validate one bounded internal publication batch size. */
    private int validateBatchSize(Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_BATCH_SIZE : requestedLimit;
        if (limit <= 0 || limit > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "job control delivery batch size must be between 1 and " + MAX_BATCH_SIZE);
        }
        return limit;
    }

    /** Require a positive non-zero duration. */
    private Duration requirePositiveDuration(Duration duration, String fieldName) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return duration;
    }

    /** Require nonblank scheduler ownership text. */
    private String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
