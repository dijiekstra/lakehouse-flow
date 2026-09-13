package io.github.lakehouseflow.model;

/**
 * Immutable outcome of evaluating one AssetDependency after an asset snapshot
 * changed.
 */
public record DependencyEvaluationOutcome(
        Long dependencyId,
        String workflowCode,
        String taskCode,
        String triggerKey,
        boolean dependencySatisfied,
        boolean schedulingIntentEmitted,
        Long workflowInstanceId,
        Long taskInstanceId,
        String resultingState,
        String reason) {

    /**
     * Build an outcome for a dependency that did not emit a scheduling intent.
     *
     * @param dependency dependency that was evaluated
     * @param triggerKey idempotent key for this snapshot decision
     * @param reason reason why scheduling was skipped
     * @return skipped dependency evaluation outcome
     */
    public static DependencyEvaluationOutcome skipped(
            AssetDependency dependency,
            String triggerKey,
            String reason) {

        return new DependencyEvaluationOutcome(
                dependency.getId(),
                dependency.getWorkflowCode(),
                dependency.getTaskCode(),
                triggerKey,
                false,
                false,
                null,
                null,
                null,
                reason);
    }

    /**
     * Build an outcome for a dependency that emitted scheduling-side instances.
     *
     * @param dependency dependency that was evaluated
     * @param triggerKey idempotent key for this snapshot decision
     * @param workflowInstance workflow instance created or reused for the decision
     * @param taskInstance task instance created or reused for the decision
     * @param resultingState scheduling-side state after the decision
     * @param reason human-readable decision explanation
     * @return emitted dependency evaluation outcome
     */
    public static DependencyEvaluationOutcome emitted(
            AssetDependency dependency,
            String triggerKey,
            WorkflowInstance workflowInstance,
            TaskInstance taskInstance,
            String resultingState,
            String reason) {

        return new DependencyEvaluationOutcome(
                dependency.getId(),
                dependency.getWorkflowCode(),
                dependency.getTaskCode(),
                triggerKey,
                true,
                true,
                workflowInstance != null ? workflowInstance.getId() : null,
                taskInstance != null ? taskInstance.getId() : null,
                resultingState,
                reason);
    }

    /**
     * Build an outcome for a dependency whose trigger key already exists.
     *
     * @param dependency dependency that matched the incoming snapshot
     * @param triggerHistory existing trigger history record
     * @return deduplicated dependency evaluation outcome
     */
    public static DependencyEvaluationOutcome deduped(
            AssetDependency dependency,
            TriggerHistory triggerHistory) {

        return new DependencyEvaluationOutcome(
                dependency.getId(),
                dependency.getWorkflowCode(),
                dependency.getTaskCode(),
                triggerHistory.getTriggerKey(),
                true,
                false,
                triggerHistory.getWorkflowInstanceId(),
                triggerHistory.getTaskInstanceId(),
                "DEDUPED",
                "Trigger already recorded");
    }
}
