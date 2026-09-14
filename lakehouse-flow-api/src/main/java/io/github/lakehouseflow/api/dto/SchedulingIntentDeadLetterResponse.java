package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * Operations response for one exhausted scheduling-intent delivery.
 *
 * @param deliveryId transport delivery id
 * @param intentId immutable scheduling intent id
 * @param intentKey downstream idempotency and snapshot attribution key
 * @param taskInstanceId task scheduling instance id
 * @param flowCode owning Flow or workflow code
 * @param taskCode scheduled node or task code
 * @param targetAssetKey target asset whose snapshot confirms the intent
 * @param bizDate business date represented by the intent
 * @param channel selected transport channel
 * @param destination selected endpoint, topic, or table
 * @param attemptCount infrastructure publication attempts
 * @param lastError latest transport error
 * @param lastAttemptAt latest infrastructure attempt time
 * @param deliverBefore publication admission deadline
 * @param deadLetteredAt time the transport entered EXHAUSTED
 */
public record SchedulingIntentDeadLetterResponse(
        Long deliveryId,
        Long intentId,
        String intentKey,
        Long taskInstanceId,
        String flowCode,
        String taskCode,
        String targetAssetKey,
        LocalDateTime bizDate,
        String channel,
        String destination,
        Integer attemptCount,
        String lastError,
        LocalDateTime lastAttemptAt,
        LocalDateTime deliverBefore,
        LocalDateTime deadLetteredAt) {
}
