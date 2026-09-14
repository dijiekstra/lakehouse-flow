package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * Operations response for one exhausted job-control intent delivery.
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
public record JobControlIntentDeadLetterResponse(
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
