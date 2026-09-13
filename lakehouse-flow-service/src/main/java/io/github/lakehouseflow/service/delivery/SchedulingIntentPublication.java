package io.github.lakehouseflow.service.delivery;

import java.util.Map;

/**
 * One fenced transport attempt for an immutable scheduling intent.
 *
 * @param deliveryId internal delivery evidence id
 * @param claimToken fencing token owned by the current scheduler process
 * @param channel selected transport channel
 * @param destination channel-specific endpoint or topic
 * @param intentId immutable scheduling intent id
 * @param intentKey downstream idempotency and snapshot-attribution key
 * @param contractVersion immutable downstream contract version
 * @param instructionPayload channel-neutral instruction body
 */
public record SchedulingIntentPublication(
        Long deliveryId,
        String claimToken,
        String channel,
        String destination,
        Long intentId,
        String intentKey,
        String contractVersion,
        Map<String, Object> instructionPayload) {
}
