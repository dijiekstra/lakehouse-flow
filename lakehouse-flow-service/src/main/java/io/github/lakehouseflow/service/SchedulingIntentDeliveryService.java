package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
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
 * Owns reliable transport state for scheduling intent publication.
 *
 * Claims and fenced updates are short database transactions. Network calls are
 * deliberately absent from this service, and none of its states are interpreted
 * as downstream task outcomes.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class SchedulingIntentDeliveryService {

    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int MAX_BATCH_SIZE = 500;
    private static final Set<String> WAITING_STATUSES = Set.of(
            SchedulingIntentDeliveryStatuses.PENDING,
            SchedulingIntentDeliveryStatuses.RETRY_WAIT);

    private final SchedulingIntentDeliveryRepository schedulingIntentDeliveryRepository;
    private final SchedulingIntentRepository schedulingIntentRepository;

    /**
     * Claim a bounded set of due delivery attempts for one scheduler process.
     *
     * An expired in-flight claim may be fenced and reclaimed. A delivery whose
     * downstream admission deadline has passed is moved to the dead-letter state
     * and is never returned to a publisher.
     *
     * @param claimOwner stable identifier for the current scheduler process
     * @param requestedLimit maximum claims, or null for the default
     * @param claimLease maximum time one process owns an attempt
     * @return immutable publications safe to perform outside the transaction
     */
    public List<SchedulingIntentPublication> claimDueDeliveries(
            String claimOwner,
            Integer requestedLimit,
            Duration claimLease) {

        String normalizedOwner = requireText(claimOwner, "claimOwner");
        int limit = validateBatchSize(requestedLimit);
        Duration normalizedLease = requirePositiveDuration(claimLease, "claimLease");
        LocalDateTime now = LocalDateTime.now();
        List<SchedulingIntentDelivery> candidates = schedulingIntentDeliveryRepository.findClaimableForUpdate(
                WAITING_STATUSES,
                SchedulingIntentDeliveryStatuses.PUBLISHING,
                now,
                PageRequest.of(0, limit));

        return candidates.stream()
                .map(delivery -> claim(delivery, normalizedOwner, normalizedLease, now))
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    /**
     * Complete one claimed publication after its transport acknowledges receipt.
     *
     * @param deliveryId claimed delivery id
     * @param claimToken fencing token returned by {@link #claimDueDeliveries}
     * @return true when the current claim was completed, otherwise false for a stale claim
     */
    public boolean recordPublished(Long deliveryId, String claimToken) {
        SchedulingIntentDelivery delivery = lockDelivery(deliveryId);
        if (SchedulingIntentDeliveryStatuses.PUBLISHED.equals(delivery.getStatus())) {
            return true;
        }
        boolean recorded = IntentDeliveryReliability.recordPublished(
                delivery,
                claimToken,
                LocalDateTime.now());
        if (recorded) {
            schedulingIntentDeliveryRepository.save(delivery);
        }
        return recorded;
    }

    /**
     * Record one failed transport attempt and schedule retry or dead-letter it.
     *
     * @param deliveryId claimed delivery id
     * @param claimToken fencing token returned by {@link #claimDueDeliveries}
     * @param error transport failure description without downstream task state
     * @param maxAttempts maximum infrastructure attempts before dead-lettering
     * @param initialBackoff delay after the first failed attempt
     * @param maximumBackoff upper bound for exponential retry delay
     * @return fenced failure outcome for metrics and transport audit
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

        SchedulingIntentDelivery delivery = lockDelivery(deliveryId);
        SchedulingIntentDeliveryFailureResult result = IntentDeliveryReliability.recordFailure(
                delivery,
                claimToken,
                error,
                maxAttempts,
                normalizedInitial,
                normalizedMaximum,
                LocalDateTime.now());
        if (result != SchedulingIntentDeliveryFailureResult.STALE) {
            schedulingIntentDeliveryRepository.save(delivery);
        }
        return result;
    }

    /** Claim one candidate or dead-letter it when its publication window ended. */
    private SchedulingIntentPublication claim(
            SchedulingIntentDelivery delivery,
            String claimOwner,
            Duration claimLease,
            LocalDateTime now) {

        java.util.Optional<String> claimToken = IntentDeliveryReliability.claim(
                delivery,
                claimOwner,
                claimLease,
                now);
        schedulingIntentDeliveryRepository.save(delivery);
        if (claimToken.isEmpty()) {
            return null;
        }

        SchedulingIntent intent = schedulingIntentRepository.findById(delivery.getSchedulingIntentId())
                .orElseThrow(() -> new IllegalStateException(
                        "Scheduling intent not found for delivery: " + delivery.getId()));

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

    /** Lock one delivery and reject invalid identifiers early. */
    private SchedulingIntentDelivery lockDelivery(Long deliveryId) {
        if (deliveryId == null || deliveryId <= 0) {
            throw new IllegalArgumentException("deliveryId must be positive");
        }
        return schedulingIntentDeliveryRepository.findByIdForUpdate(deliveryId)
                .orElseThrow(() -> new IllegalArgumentException("Scheduling intent delivery not found: " + deliveryId));
    }

    /** Validate and normalize a scheduler-owned claim batch size. */
    private int validateBatchSize(Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_BATCH_SIZE : requestedLimit;
        if (limit <= 0 || limit > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("delivery batch size must be between 1 and " + MAX_BATCH_SIZE);
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

    /** Require non-blank scheduler configuration text. */
    private String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

}
