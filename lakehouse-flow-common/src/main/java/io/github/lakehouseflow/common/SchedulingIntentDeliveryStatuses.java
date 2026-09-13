package io.github.lakehouseflow.common;

/**
 * Transport-only lifecycle states for scheduling intent publication.
 *
 * These values never represent downstream execution outcomes.
 */
public final class SchedulingIntentDeliveryStatuses {

    /** Intent is waiting for an internal transport publisher. */
    public static final String PENDING = "PENDING";

    /** One scheduler process currently owns the transport publication lease. */
    public static final String PUBLISHING = "PUBLISHING";

    /** Transport accepted the intent or the database outbox row became visible. */
    public static final String PUBLISHED = "PUBLISHED";

    /** A transport attempt failed and may be retried later. */
    public static final String RETRY_WAIT = "RETRY_WAIT";

    /**
     * Transport retry policy or publication deadline was exhausted.
     *
     * This is the delivery dead-letter state and does not imply that a
     * downstream task ran or failed.
     */
    public static final String EXHAUSTED = "EXHAUSTED";

    /** Prevent utility-class construction. */
    private SchedulingIntentDeliveryStatuses() {
    }
}
