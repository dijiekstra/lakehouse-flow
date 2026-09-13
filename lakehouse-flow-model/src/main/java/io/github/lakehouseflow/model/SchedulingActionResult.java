package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SchedulingActionStatuses;

import java.util.List;

/**
 * Immutable service result for a scheduling action request.
 */
public record SchedulingActionResult(
        String actionKey,
        String actionType,
        String status,
        String message,
        List<Long> workflowInstanceIds,
        Long taskInstanceId) {

    /**
     * Build a result for an action that was applied to scheduling records.
     *
     * @param action persisted action request
     * @param message human-readable result message
     * @param workflowInstanceIds workflow instances created or affected
     * @param taskInstanceId task instance affected by the action
     * @return applied action result
     */
    public static SchedulingActionResult applied(
            SchedulingAction action,
            String message,
            List<Long> workflowInstanceIds,
            Long taskInstanceId) {

        return new SchedulingActionResult(
                action.getActionKey(),
                action.getActionType(),
                SchedulingActionStatuses.APPLIED,
                message,
                List.copyOf(workflowInstanceIds),
                taskInstanceId);
    }

    /**
     * Build a result for a duplicate idempotency key.
     *
     * @param action existing action request
     * @return deduplicated action result
     */
    public static SchedulingActionResult deduped(SchedulingAction action) {
        List<Long> workflowIds = action.getProducedWorkflowInstanceId() == null
                ? List.of()
                : List.of(action.getProducedWorkflowInstanceId());
        return new SchedulingActionResult(
                action.getActionKey(),
                action.getActionType(),
                SchedulingActionStatuses.DEDUPED,
                "Action already recorded with status " + action.getStatus(),
                workflowIds,
                action.getProducedTaskInstanceId());
    }

    /**
     * Build a result for an action rejected by scheduler validation.
     *
     * @param action persisted action request
     * @param message human-readable rejection reason
     * @return rejected action result
     */
    public static SchedulingActionResult rejected(SchedulingAction action, String message) {
        return new SchedulingActionResult(
                action.getActionKey(),
                action.getActionType(),
                SchedulingActionStatuses.REJECTED,
                message,
                List.of(),
                action.getTaskInstanceId());
    }
}
