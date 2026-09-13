package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SchedulingStates;

/**
 * Immutable outcome of one target snapshot confirmation check.
 */
public record SnapshotConfirmationResult(
        Long taskInstanceId,
        String targetAssetKey,
        String baselineSnapshotId,
        String observedSnapshotId,
        String resultingState,
        boolean snapshotAdvanced,
        boolean confirmationExpired,
        String waitingReason) {

    /**
     * Build a result for a task whose target snapshot advanced.
     *
     * @param task scheduled task being confirmed
     * @param observedSnapshotId latest observed target snapshot id
     * @return confirmed snapshot result
     */
    public static SnapshotConfirmationResult confirmed(TaskInstance task, String observedSnapshotId) {
        return new SnapshotConfirmationResult(
                task.getId(),
                task.getTargetAssetKey(),
                task.getBaselineSnapshotId(),
                observedSnapshotId,
                SchedulingStates.SNAPSHOT_CONFIRMED,
                true,
                false,
                null);
    }

    /**
     * Build a result for a task that is still waiting for target snapshot progress.
     *
     * @param task scheduled task being checked
     * @param observedSnapshotId latest observed target snapshot id
     * @param waitingReason explanation of why progress is not confirmed
     * @return non-terminal waiting snapshot result
     */
    public static SnapshotConfirmationResult waiting(
            TaskInstance task,
            String observedSnapshotId,
            String waitingReason) {

        return new SnapshotConfirmationResult(
                task.getId(),
                task.getTargetAssetKey(),
                task.getBaselineSnapshotId(),
                observedSnapshotId,
                SchedulingStates.SCHEDULED,
                false,
                false,
                waitingReason);
    }

    /**
     * Build a result for a task whose confirmation window expired.
     *
     * @param task scheduled task being checked
     * @param observedSnapshotId latest observed target snapshot id
     * @param waitingReason explanation of the missing snapshot progress
     * @return expired snapshot confirmation result
     */
    public static SnapshotConfirmationResult expired(
            TaskInstance task,
            String observedSnapshotId,
            String waitingReason) {

        return new SnapshotConfirmationResult(
                task.getId(),
                task.getTargetAssetKey(),
                task.getBaselineSnapshotId(),
                observedSnapshotId,
                SchedulingStates.SNAPSHOT_NOT_ADVANCED,
                false,
                true,
                waitingReason);
    }
}
