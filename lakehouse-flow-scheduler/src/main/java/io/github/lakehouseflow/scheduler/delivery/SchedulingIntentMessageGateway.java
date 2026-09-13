package io.github.lakehouseflow.scheduler.delivery;

import java.util.Map;

/**
 * Broker-neutral port implemented by a deployment's Kafka, Pulsar, or RabbitMQ adapter.
 */
public interface SchedulingIntentMessageGateway {

    /**
     * Publish one immutable scheduling intent to a configured broker destination.
     *
     * A normal return means broker acceptance only and must not represent a task
     * outcome. Implementations must preserve the key for downstream idempotency.
     *
     * @param destination broker topic, subject, or routing destination
     * @param messageKey immutable scheduling intent key
     * @param headers transport metadata without runtime state
     * @param payload serialized channel-neutral instruction payload
     */
    void publish(String destination, String messageKey, Map<String, String> headers, byte[] payload);
}
