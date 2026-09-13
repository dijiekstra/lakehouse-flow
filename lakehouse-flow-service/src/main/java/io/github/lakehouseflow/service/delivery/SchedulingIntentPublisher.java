package io.github.lakehouseflow.service.delivery;

/**
 * Transport SPI used by the internal scheduler to push immutable intents.
 *
 * Implementations only report whether the transport accepted the instruction.
 * They must never query or return downstream task runtime state.
 */
public interface SchedulingIntentPublisher {

    /**
     * Return the single transport channel handled by this publisher.
     *
     * @return channel identifier from the scheduling intent delivery contract
     */
    String channel();

    /**
     * Publish exactly the immutable instruction payload in one claimed attempt.
     *
     * Implementations should throw when the transport does not acknowledge the
     * instruction. A normal return means transport acceptance only.
     *
     * @param publication fenced immutable publication request
     */
    void publish(SchedulingIntentPublication publication);
}
