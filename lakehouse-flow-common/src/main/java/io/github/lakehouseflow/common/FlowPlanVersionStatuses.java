package io.github.lakehouseflow.common;

/**
 * Lifecycle statuses for one version of a FlowPlan.
 *
 * Versions are the stable definition anchor used by scheduling instances and
 * action audit records. Publishing a version does not execute work; it only
 * makes the definition eligible for future scheduling decisions.
 */
public final class FlowPlanVersionStatuses {

    public static final String DRAFT = "DRAFT";
    public static final String PUBLISHED = "PUBLISHED";
    public static final String RETIRED = "RETIRED";

    /**
     * Prevent utility class instantiation.
     */
    private FlowPlanVersionStatuses() {
    }

    /**
     * Check whether a version is published.
     *
     * @param status FlowPlanVersion lifecycle status
     * @return true when the version is published
     */
    public static boolean isPublished(String status) {
        return PUBLISHED.equals(status);
    }
}
