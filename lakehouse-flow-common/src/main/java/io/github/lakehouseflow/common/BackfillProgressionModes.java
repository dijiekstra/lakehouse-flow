package io.github.lakehouseflow.common;

/**
 * Scheduler-side date progression modes for a backfill batch.
 *
 * These modes control how many business-date scheduling instances may have
 * deliverable intents at once. They do not describe downstream execution
 * parallelism or resource allocation.
 */
public final class BackfillProgressionModes {

    public static final String PARALLEL = "PARALLEL";
    public static final String SERIAL = "SERIAL";
    public static final String PARALLEL_WITH_LIMIT = "PARALLEL_WITH_LIMIT";

    /**
     * Prevent utility class instantiation.
     */
    private BackfillProgressionModes() {
    }
}
