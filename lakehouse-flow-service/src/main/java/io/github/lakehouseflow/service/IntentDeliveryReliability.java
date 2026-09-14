package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.model.ReliableIntentDelivery;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Shared claim, fencing, retry, and dead-letter algorithm for outbound intents.
 */
final class IntentDeliveryReliability {

    private static final int MAX_ERROR_LENGTH = 4000;

    /** Prevent construction of this stateless algorithm holder. */
    private IntentDeliveryReliability() {
    }

    /** Claim one due record or dead-letter it when external admission expired. */
    static Optional<String> claim(
            ReliableIntentDelivery delivery,
            String claimOwner,
            Duration claimLease,
            LocalDateTime now) {
        if (deadlineReached(delivery, now)) {
            deadLetter(delivery, now, "Publication admission expired before transport acknowledgement");
            return Optional.empty();
        }
        String claimToken = UUID.randomUUID().toString();
        delivery.setStatus(SchedulingIntentDeliveryStatuses.PUBLISHING);
        delivery.setAttemptCount(normalizeAttemptCount(delivery.getAttemptCount()) + 1);
        delivery.setLastAttemptAt(now);
        delivery.setNextAttemptAt(null);
        delivery.setClaimOwner(claimOwner);
        delivery.setClaimToken(claimToken);
        delivery.setClaimExpiresAt(now.plus(claimLease));
        return Optional.of(claimToken);
    }

    /** Complete a transport acknowledgement only for the current fenced claim. */
    static boolean recordPublished(
            ReliableIntentDelivery delivery,
            String claimToken,
            LocalDateTime now) {
        if (SchedulingIntentDeliveryStatuses.PUBLISHED.equals(delivery.getStatus())) {
            return true;
        }
        if (!ownsClaim(delivery, claimToken)) {
            return false;
        }
        delivery.setStatus(SchedulingIntentDeliveryStatuses.PUBLISHED);
        delivery.setPublishedAt(now);
        delivery.setLastError(null);
        delivery.setNextAttemptAt(null);
        delivery.setDeadLetteredAt(null);
        clearClaim(delivery);
        return true;
    }

    /** Apply bounded retry or terminal exhaustion for the current fenced claim. */
    static SchedulingIntentDeliveryFailureResult recordFailure(
            ReliableIntentDelivery delivery,
            String claimToken,
            String error,
            int maxAttempts,
            Duration initialBackoff,
            Duration maximumBackoff,
            LocalDateTime now) {
        if (!ownsClaim(delivery, claimToken)) {
            return SchedulingIntentDeliveryFailureResult.STALE;
        }
        delivery.setLastError(truncateError(error));
        clearClaim(delivery);
        int attemptCount = normalizeAttemptCount(delivery.getAttemptCount());
        delivery.setAttemptCount(attemptCount);
        if (attemptCount >= maxAttempts || deadlineReached(delivery, now)) {
            deadLetter(delivery, now, delivery.getLastError());
            return SchedulingIntentDeliveryFailureResult.EXHAUSTED;
        }
        delivery.setStatus(SchedulingIntentDeliveryStatuses.RETRY_WAIT);
        delivery.setNextAttemptAt(now.plus(calculateBackoff(
                attemptCount,
                initialBackoff,
                maximumBackoff)));
        return SchedulingIntentDeliveryFailureResult.RETRY_SCHEDULED;
    }

    /** Check that a completion belongs to the current in-flight claim. */
    private static boolean ownsClaim(ReliableIntentDelivery delivery, String claimToken) {
        return SchedulingIntentDeliveryStatuses.PUBLISHING.equals(delivery.getStatus())
                && claimToken != null
                && claimToken.equals(delivery.getClaimToken());
    }

    /** Clear ephemeral claim ownership after an attempt. */
    private static void clearClaim(ReliableIntentDelivery delivery) {
        delivery.setClaimOwner(null);
        delivery.setClaimToken(null);
        delivery.setClaimExpiresAt(null);
    }

    /** Move one transport to its terminal dead-letter audit state. */
    private static void deadLetter(
            ReliableIntentDelivery delivery,
            LocalDateTime now,
            String reason) {
        clearClaim(delivery);
        delivery.setStatus(SchedulingIntentDeliveryStatuses.EXHAUSTED);
        delivery.setLastError(truncateError(reason));
        delivery.setNextAttemptAt(null);
        delivery.setDeadLetteredAt(now);
    }

    /** Determine whether the external publication admission window ended. */
    private static boolean deadlineReached(ReliableIntentDelivery delivery, LocalDateTime now) {
        return delivery.getDeliverBefore() != null && !now.isBefore(delivery.getDeliverBefore());
    }

    /** Calculate bounded exponential backoff from the completed attempt number. */
    private static Duration calculateBackoff(
            int attemptCount,
            Duration initialBackoff,
            Duration maximumBackoff) {
        int exponent = Math.max(0, Math.min(attemptCount - 1, 30));
        long multiplier = 1L << exponent;
        try {
            Duration calculated = initialBackoff.multipliedBy(multiplier);
            return calculated.compareTo(maximumBackoff) > 0 ? maximumBackoff : calculated;
        } catch (ArithmeticException ignored) {
            return maximumBackoff;
        }
    }

    /** Normalize a nullable legacy attempt counter. */
    private static int normalizeAttemptCount(Integer attemptCount) {
        return attemptCount == null ? 0 : attemptCount;
    }

    /** Keep transport errors inside the database audit bound. */
    private static String truncateError(String error) {
        String normalized = error == null || error.isBlank() ? "Unknown transport failure" : error.trim();
        return normalized.length() <= MAX_ERROR_LENGTH
                ? normalized
                : normalized.substring(0, MAX_ERROR_LENGTH);
    }
}
