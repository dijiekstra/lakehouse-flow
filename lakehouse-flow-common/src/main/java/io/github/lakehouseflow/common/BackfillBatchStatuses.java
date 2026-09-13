package io.github.lakehouseflow.common;

/**
 * Scheduler-side states for a backfill batch.
 *
 * A backfill batch describes how Lakehouse Flow expanded a historical range
 * into scheduling intents. It does not represent external execution status.
 */
public final class BackfillBatchStatuses {

    public static final String CREATED = "CREATED";
    public static final String EXPANDED = "EXPANDED";
    public static final String FAILED = "FAILED";
    public static final String PAUSED = "PAUSED";
    public static final String CANCELLED = "CANCELLED";
    public static final String COMPLETED = "COMPLETED";

    /**
     * Prevent utility class instantiation.
     */
    private BackfillBatchStatuses() {
    }
}
