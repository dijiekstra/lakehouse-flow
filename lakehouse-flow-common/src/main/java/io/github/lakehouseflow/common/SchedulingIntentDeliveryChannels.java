package io.github.lakehouseflow.common;

/**
 * Supported transport channels for scheduler-owned intent publication.
 */
public final class SchedulingIntentDeliveryChannels {

    /** Durable database outbox consumed by downstream integrations. */
    public static final String DATABASE_TABLE = "DATABASE_TABLE";

    /** Downstream HTTP endpoint invoked by an internal publisher. */
    public static final String HTTP = "HTTP";

    /** Message broker topic published by an internal publisher. */
    public static final String MQ = "MQ";

    /** Prevent utility-class construction. */
    private SchedulingIntentDeliveryChannels() {
    }
}
