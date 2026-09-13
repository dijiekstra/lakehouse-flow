package io.github.lakehouseflow.common;

/**
 * Scheduling-side action types supported by Lakehouse Flow.
 *
 * These actions change scheduling records or emit new scheduling decisions.
 * They never execute external jobs directly.
 */
public final class SchedulingActionTypes {

    public static final String RERUN_WORKFLOW = "RERUN_WORKFLOW";
    public static final String RERUN_TASK = "RERUN_TASK";
    public static final String BACKFILL_WORKFLOW = "BACKFILL_WORKFLOW";
    public static final String BACKFILL_NODE = "BACKFILL_NODE";
    public static final String RECOVER_BACKFILL = "RECOVER_BACKFILL";
    public static final String PAUSE_BACKFILL = "PAUSE_BACKFILL";
    public static final String RESUME_BACKFILL = "RESUME_BACKFILL";
    public static final String CANCEL_BACKFILL = "CANCEL_BACKFILL";
    public static final String CANCEL_WORKFLOW = "CANCEL_WORKFLOW";
    public static final String CANCEL_TASK = "CANCEL_TASK";
    public static final String SKIP_TASK = "SKIP_TASK";
    public static final String RECHECK_SNAPSHOT = "RECHECK_SNAPSHOT";

    /**
     * Prevent utility class instantiation.
     */
    private SchedulingActionTypes() {
    }

    /**
     * Map an action type to the trigger type stored on generated workflow instances.
     *
     * @param actionType scheduling action type
     * @return workflow trigger type for instances created by this action
     */
    public static String toTriggerType(String actionType) {
        return switch (actionType) {
            case RERUN_WORKFLOW -> "RERUN";
            case RERUN_TASK -> "RERUN_TASK";
            case BACKFILL_WORKFLOW, BACKFILL_NODE -> "BACKFILL";
            case RECOVER_BACKFILL -> "BACKFILL_RECOVERY";
            default -> "MANUAL_ACTION";
        };
    }

    /**
     * Check whether an action emits a new workflow scheduling decision.
     *
     * @param actionType scheduling action type
     * @return true when the action creates workflow instances
     */
    public static boolean createsWorkflowInstance(String actionType) {
        return RERUN_WORKFLOW.equals(actionType)
                || RERUN_TASK.equals(actionType)
                || BACKFILL_WORKFLOW.equals(actionType)
                || BACKFILL_NODE.equals(actionType)
                || RECOVER_BACKFILL.equals(actionType);
    }
}
