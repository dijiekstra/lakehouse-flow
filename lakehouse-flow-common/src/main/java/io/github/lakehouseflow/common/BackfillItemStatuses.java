package io.github.lakehouseflow.common;

/**
 * Scheduler-side states for one expanded backfill item.
 *
 * Backfill items point to generated scheduling intents. They do not track
 * executor runtime state.
 */
public final class BackfillItemStatuses {

    public static final String CREATED = "CREATED";
    public static final String WAITING_CONCURRENCY = "WAITING_CONCURRENCY";
    public static final String WAITING_DEPENDENCY = "WAITING_DEPENDENCY";
    public static final String INTENT_READY = "INTENT_READY";
    public static final String INTENT_DELIVERED = "INTENT_DELIVERED";
    public static final String SNAPSHOT_CONFIRMED = "SNAPSHOT_CONFIRMED";
    public static final String SNAPSHOT_NOT_ADVANCED = "SNAPSHOT_NOT_ADVANCED";
    public static final String CANCELLED = "CANCELLED";

    /**
     * Prevent utility class instantiation.
     */
    private BackfillItemStatuses() {
    }
}
