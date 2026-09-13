package io.github.lakehouseflow.common;

/**
 * Processing states for scheduling-side actions.
 *
 * Action status describes whether Lakehouse Flow accepted and applied a
 * scheduling request. It is not an external task runtime status.
 */
public final class SchedulingActionStatuses {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String APPLIED = "APPLIED";
    public static final String DEDUPED = "DEDUPED";
    public static final String REJECTED = "REJECTED";

    /**
     * Prevent utility class instantiation.
     */
    private SchedulingActionStatuses() {
    }

    /**
     * Check whether an action state is terminal for request handling.
     *
     * @param status action processing status
     * @return true when no more processing is expected for the action request
     */
    public static boolean isTerminal(String status) {
        return APPLIED.equals(status) || DEDUPED.equals(status) || REJECTED.equals(status);
    }
}
