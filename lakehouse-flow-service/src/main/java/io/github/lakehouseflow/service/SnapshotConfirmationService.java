package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SnapshotConfirmationResult;
import io.github.lakehouseflow.model.TaskInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Drives scheduled task confirmation from target asset snapshot evidence.
 *
 * Lakehouse Flow does not execute tasks. This service confirms only a target
 * snapshot that advanced beyond the frozen baseline and carries evidence for
 * the immutable scheduling intent. It never consumes downstream runtime
 * feedback.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class SnapshotConfirmationService {

    private static final String RELEASE_REASON_CONFIRMED = "SNAPSHOT_CONFIRMED";
    private static final String RELEASE_REASON_EXPIRED = "SNAPSHOT_CONFIRMATION_EXPIRED";

    private final TaskInstanceRepository taskInstanceRepository;
    private final SchedulingIntentRepository schedulingIntentRepository;
    private final TaskInstanceService taskInstanceService;
    private final SnapshotEvidenceService snapshotEvidenceService;
    private final DagProgressionService dagProgressionService;
    private final SchedulingTargetAdmissionService schedulingTargetAdmissionService;
    private final FlowPlanPolicyService flowPlanPolicyService;
    private final SnapshotSourceHealthService snapshotSourceHealthService;
    private final WriterJobBindingService writerJobBindingService;

    /**
     * Check one scheduled task and update its scheduling-side state when the
     * target snapshot provides attributable progress evidence or the
     * confirmation window expires.
     */
    public SnapshotConfirmationResult checkTaskSnapshotProgress(Long taskId, Duration confirmationTimeout) {
        TaskInstance task = getTask(taskId);
        if (SchedulingStates.SNAPSHOT_CONFIRMED.equals(task.getState())) {
            return SnapshotConfirmationResult.confirmed(task, task.getObservedSnapshotId());
        }
        if (!SchedulingStates.SCHEDULED.equals(task.getState())) {
            throw new IllegalStateException("Task " + taskId + " is not scheduled: " + task.getState());
        }
        if (isBlank(task.getTargetAssetKey())) {
            throw new IllegalStateException("Task " + taskId + " has no target asset for snapshot confirmation");
        }

        SchedulingIntent intent = schedulingIntentRepository.findByTaskInstanceId(taskId)
                .orElseThrow(() -> new IllegalStateException(
                        "Scheduled task has no immutable scheduling intent: " + taskId));
        EvaluationResult result = snapshotEvidenceService.evaluateIntentProgress(intent);

        String observedSnapshotId = result.getSnapshotId();
        if (Boolean.TRUE.equals(result.getSatisfied())) {
            taskInstanceService.confirmSnapshotProgress(
                    taskId,
                    task.getTargetAssetKey(),
                    task.getBaselineSnapshotId(),
                    observedSnapshotId);
            dagProgressionService.onSnapshotConfirmed(taskId);
            releaseTargetAdmission(intent, RELEASE_REASON_CONFIRMED);
            releaseWriterAdmission(intent);
            return SnapshotConfirmationResult.confirmed(task, observedSnapshotId);
        }

        String waitingReason = result.getWaitingReason();
        Duration effectiveTimeout = confirmationTimeout == null
                ? null
                : flowPlanPolicyService.resolve(task, confirmationTimeout, confirmationTimeout)
                        .confirmationTimeout();
        if (isConfirmationExpired(task, effectiveTimeout)) {
            LocalDateTime deadline = confirmationDeadline(task, effectiveTimeout);
            SnapshotSourceHealthService.SourceHealthDecision sourceDecision =
                    snapshotSourceHealthService.evaluateTimeoutEvidence(task.getTargetAssetKey(), deadline);
            if (!sourceDecision.timeoutConclusionAllowed()) {
                String sourceBlockedReason = sourceBlockedReason(sourceDecision, waitingReason);
                taskInstanceService.recordSnapshotCheck(
                        taskId,
                        task.getTargetAssetKey(),
                        task.getBaselineSnapshotId(),
                        observedSnapshotId,
                        sourceBlockedReason);
                return SnapshotConfirmationResult.sourceBlocked(
                        task,
                        observedSnapshotId,
                        sourceBlockedReason,
                        sourceDecision.sourceHealth(),
                        sourceDecision.detail(),
                        sourceDecision.evidenceCheckedAt());
            }
            taskInstanceService.markSnapshotNotAdvanced(
                    taskId,
                    task.getTargetAssetKey(),
                    task.getBaselineSnapshotId(),
                    observedSnapshotId,
                    waitingReason);
            dagProgressionService.onSnapshotNotAdvanced(taskId);
            releaseTargetAdmission(intent, RELEASE_REASON_EXPIRED);
            releaseWriterAdmission(intent);
            return SnapshotConfirmationResult.expired(
                    task,
                    observedSnapshotId,
                    waitingReason,
                    sourceDecision.sourceHealth(),
                    sourceDecision.detail(),
                    sourceDecision.evidenceCheckedAt());
        }

        taskInstanceService.recordSnapshotCheck(
                taskId,
                task.getTargetAssetKey(),
                task.getBaselineSnapshotId(),
                observedSnapshotId,
                waitingReason);
        return SnapshotConfirmationResult.waiting(task, observedSnapshotId, waitingReason);
    }

    /**
     * Check all scheduled tasks with target assets.
     */
    public List<SnapshotConfirmationResult> checkScheduledTasks(Duration confirmationTimeout) {
        return taskInstanceRepository.findScheduledAwaitingSnapshotConfirmation().stream()
                .map(task -> checkTaskSnapshotProgress(task.getId(), confirmationTimeout))
                .toList();
    }

    /**
     * Load a task instance or fail fast with a scheduler-domain error.
     *
     * @param taskId task instance id
     * @return persisted task instance
     */
    private TaskInstance getTask(Long taskId) {
        return taskInstanceRepository.findByIdForUpdate(taskId)
                .orElseThrow(() -> new RuntimeException("Task instance not found: " + taskId));
    }

    /**
     * Release the target-date slot only if this confirmed intent still owns it.
     *
     * @param intent immutable scheduling intent being confirmed
     * @param reason scheduler-side terminal release reason
     */
    private void releaseTargetAdmission(SchedulingIntent intent, String reason) {
        if (intent.getBizDate() == null) {
            throw new IllegalStateException(
                    "Scheduling intent has no business date for target admission release: "
                            + intent.getId());
        }
        schedulingTargetAdmissionService.release(
                intent.getTargetAssetKey(),
                intent.getBizDate().toLocalDate(),
                intent.getTaskInstanceId(),
                reason);
    }

    /** Release a bounded batch writer only when this exact intent still owns its epoch. */
    private void releaseWriterAdmission(SchedulingIntent intent) {
        writerJobBindingService.releaseDataIntent(
                intent.getWriterJobKey(),
                intent.getWriterEpoch(),
                intent.getIntentKey());
    }

    /**
     * Determine whether a scheduled task exceeded the confirmation window.
     *
     * @param task scheduled task being checked
     * @param confirmationTimeout maximum time allowed for target snapshot progress
     * @return true when the task should be marked as snapshot-not-advanced
     */
    private boolean isConfirmationExpired(TaskInstance task, Duration confirmationTimeout) {
        LocalDateTime expiresAt = confirmationDeadline(task, confirmationTimeout);
        if (expiresAt == null) {
            return false;
        }
        return !LocalDateTime.now().isBefore(expiresAt);
    }

    /**
     * Resolve the immutable end of a task's snapshot confirmation window.
     *
     * @param task scheduled task
     * @param confirmationTimeout effective timeout
     * @return confirmation deadline, or null when timeout is disabled
     */
    private LocalDateTime confirmationDeadline(TaskInstance task, Duration confirmationTimeout) {
        if (confirmationTimeout == null || task.getScheduledAt() == null) {
            return null;
        }
        return task.getScheduledAt().plus(confirmationTimeout);
    }

    /**
     * Combine snapshot waiting evidence with the independent source-blocking reason.
     *
     * @param sourceDecision persisted source-health decision
     * @param snapshotWaitingReason current attributable-snapshot waiting reason
     * @return scheduler-side non-terminal wait explanation
     */
    private String sourceBlockedReason(
            SnapshotSourceHealthService.SourceHealthDecision sourceDecision,
            String snapshotWaitingReason) {
        String snapshotDetail = isBlank(snapshotWaitingReason) ? "target snapshot evidence is absent" : snapshotWaitingReason;
        String sourceDetail = isBlank(sourceDecision.detail()) ? "source evidence is incomplete" : sourceDecision.detail();
        return "SOURCE_BLOCKED [" + sourceDecision.sourceHealth() + "]: " + sourceDetail
                + "; snapshot=" + snapshotDetail;
    }

    /**
     * Check whether a string is null or only whitespace.
     *
     * @param value source value
     * @return true when value has no meaningful text
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
