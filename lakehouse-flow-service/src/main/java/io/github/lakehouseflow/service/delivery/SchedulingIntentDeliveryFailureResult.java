package io.github.lakehouseflow.service.delivery;

/**
 * Result of applying one fenced scheduling-intent transport failure.
 */
public enum SchedulingIntentDeliveryFailureResult {

    /** The failure belonged to an expired or replaced claim and was ignored. */
    STALE,

    /** The delivery was moved to retry wait with a bounded backoff. */
    RETRY_SCHEDULED,

    /** The delivery reached its retry or publication deadline and was dead-lettered. */
    EXHAUSTED
}
