package io.github.lakehouseflow.common;

/**
 * Lifecycle statuses for a scheduler-side FlowPlan.
 *
 * A FlowPlan is the isolation and ownership boundary for scheduling
 * definitions. Its status controls whether published versions may be used for
 * scheduling decisions, not whether external work is running.
 */
public final class FlowPlanStatuses {

    public static final String DRAFT = "DRAFT";
    public static final String PUBLISHED = "PUBLISHED";
    public static final String SUSPENDED = "SUSPENDED";
    public static final String RETIRED = "RETIRED";

    /**
     * Prevent utility class instantiation.
     */
    private FlowPlanStatuses() {
    }

    /**
     * Check whether a FlowPlan may participate in scheduling decisions.
     *
     * @param status FlowPlan lifecycle status
     * @return true when published definitions can be selected for scheduling
     */
    public static boolean isSchedulable(String status) {
        return PUBLISHED.equals(status);
    }
}
