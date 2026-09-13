package io.github.lakehouseflow.dao;

/**
 * Grouped delivery-state count used to refresh low-cost backlog gauges.
 */
public interface SchedulingIntentDeliveryStatusCount {

    /**
     * Return the selected transport channel.
     *
     * @return delivery channel
     */
    String getChannel();

    /**
     * Return the transport-only delivery status.
     *
     * @return delivery status
     */
    String getStatus();

    /**
     * Return the number of deliveries in this channel and status.
     *
     * @return grouped delivery count
     */
    long getDeliveryCount();
}
