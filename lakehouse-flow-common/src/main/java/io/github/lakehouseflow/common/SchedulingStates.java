package io.github.lakehouseflow.common;

/**
 * Scheduling-side instance states.
 *
 * Lakehouse Flow does not own task execution. These states describe the
 * scheduler's decision and the observed target snapshot evidence, not an
 * executor runtime lifecycle.
 */
public final class SchedulingStates {

    /**
     * Prevent utility class instantiation.
     */
    private SchedulingStates() {
    }

    public static final String CREATED = "CREATED";
    public static final String WAITING_SNAPSHOT = "WAITING_SNAPSHOT";
    public static final String READY_TO_SCHEDULE = "READY_TO_SCHEDULE";
    public static final String SCHEDULED = "SCHEDULED";
    public static final String SNAPSHOT_CONFIRMED = "SNAPSHOT_CONFIRMED";
    public static final String SNAPSHOT_NOT_ADVANCED = "SNAPSHOT_NOT_ADVANCED";
    public static final String CANCELLED = "CANCELLED";
    public static final String SKIPPED = "SKIPPED";

    /**
     * Check whether a state represents final snapshot evidence.
     *
     * @param state scheduling-side state value
     * @return true when the state records target snapshot confirmation or non-progression
     */
    public static boolean isSnapshotOutcome(String state) {
        return SNAPSHOT_CONFIRMED.equals(state) || SNAPSHOT_NOT_ADVANCED.equals(state);
    }
}
