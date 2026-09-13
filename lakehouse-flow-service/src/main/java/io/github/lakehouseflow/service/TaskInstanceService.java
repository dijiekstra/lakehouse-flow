package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.TaskInstance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing task scheduling instances.
 *
 * State transitions:
 * CREATED → WAITING_SNAPSHOT → READY_TO_SCHEDULE → SCHEDULED → SNAPSHOT_CONFIRMED
 *                                                    ↘ SKIPPED   ↘ SNAPSHOT_NOT_ADVANCED
 *
 * This service records Lakehouse Flow's scheduling decision and later snapshot
 * evidence. It does not dispatch jobs or track executor runtime status.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class TaskInstanceService {

    private final TaskInstanceRepository taskInstanceRepository;

    /**
     * Create a new task scheduling instance.
     *
     * Idempotency: instance_key ensures duplicate creation is prevented.
     */
    public TaskInstance createInstance(
            Long workflowInstanceId,
            String taskCode,
            Integer taskVersion,
            LocalDateTime bizDate) {

        return createInstance(workflowInstanceId, taskCode, taskVersion, bizDate, null);
    }

    /**
     * Create a new task scheduling instance with an optional target asset.
     *
     * The target asset is copied into the scheduling decision so the internal
     * outbox publisher can freeze its baseline before publication.
     *
     * @param workflowInstanceId owning workflow scheduling instance
     * @param taskCode task or node code
     * @param taskVersion task definition version
     * @param bizDate business date associated with the scheduling decision
     * @param targetAssetKey target asset used for later snapshot confirmation
     * @return newly created or existing task scheduling instance
     */
    public TaskInstance createInstance(
            Long workflowInstanceId,
            String taskCode,
            Integer taskVersion,
            LocalDateTime bizDate,
            String targetAssetKey) {

        return createInstance(
                workflowInstanceId,
                taskCode,
                taskVersion,
                bizDate,
                targetAssetKey,
                null,
                null);
    }

    /**
     * Create a task scheduling instance with optional definition anchors.
     *
     * FlowPlanVersion and ScheduleNode links make emitted intents traceable back
     * to the published model. They are not execution handles and do not change
     * the snapshot-based confirmation semantics.
     *
     * @param workflowInstanceId owning workflow scheduling instance
     * @param taskCode task or node code
     * @param taskVersion task definition version
     * @param bizDate business date associated with the scheduling decision
     * @param targetAssetKey target asset used for later snapshot confirmation
     * @param flowPlanVersionId optional FlowPlanVersion id that produced this task
     * @param scheduleNodeId optional ScheduleNode id that produced this task
     * @return newly created or existing task scheduling instance
     */
    public TaskInstance createInstance(
            Long workflowInstanceId,
            String taskCode,
            Integer taskVersion,
            LocalDateTime bizDate,
            String targetAssetKey,
            Long flowPlanVersionId,
            Long scheduleNodeId) {

        String instanceKey = buildInstanceKey(workflowInstanceId, taskCode);

        // Check if already exists
        Optional<TaskInstance> existing = taskInstanceRepository.findByInstanceKey(instanceKey);
        if (existing.isPresent()) {
            log.debug("Task instance {} already exists, skipping creation", instanceKey);
            return existing.get();
        }

        log.info("Creating task instance {} for workflow {}", instanceKey, workflowInstanceId);

        TaskInstance instance = TaskInstance.builder()
                .instanceKey(instanceKey)
                .workflowInstanceId(workflowInstanceId)
                .taskCode(taskCode)
                .taskVersion(taskVersion)
                .flowPlanVersionId(flowPlanVersionId)
                .scheduleNodeId(scheduleNodeId)
                .bizDate(bizDate)
                .state(SchedulingStates.CREATED)
                .targetAssetKey(targetAssetKey)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        return taskInstanceRepository.save(instance);
    }

    /**
     * Transition task scheduling state with validation.
     */
    public void transitionState(Long taskId, String newState, String waitingReason) {
        Optional<TaskInstance> task = taskInstanceRepository.findById(taskId);
        if (task.isEmpty()) {
            throw new RuntimeException("Task instance not found: " + taskId);
        }

        TaskInstance t = task.get();
        String oldState = t.getState();

        // Validate state transition
        if (!isValidTransition(oldState, newState)) {
            log.warn("Invalid state transition from {} to {}", oldState, newState);
            throw new RuntimeException("Invalid state transition: " + oldState + " -> " + newState);
        }

        t.setState(newState);
        t.setWaitingReason(waitingReason);

        LocalDateTime now = LocalDateTime.now();
        if (SchedulingStates.SCHEDULED.equals(newState)) {
            t.setScheduledAt(now);
        } else if (SchedulingStates.isSnapshotOutcome(newState)) {
            t.setLastSnapshotCheckAt(now);
        }

        taskInstanceRepository.save(t);
        log.info("Task instance {} transitioned from {} to {}", t.getInstanceKey(), oldState, newState);
    }

    /**
     * Validate state transition
     */
    boolean isValidTransition(String fromState, String toState) {
        return switch (fromState) {
            case SchedulingStates.CREATED -> toState.equals(SchedulingStates.WAITING_SNAPSHOT)
                    || toState.equals(SchedulingStates.READY_TO_SCHEDULE)
                    || toState.equals(SchedulingStates.CANCELLED)
                    || toState.equals(SchedulingStates.SKIPPED);
            case SchedulingStates.WAITING_SNAPSHOT -> toState.equals(SchedulingStates.READY_TO_SCHEDULE)
                    || toState.equals(SchedulingStates.CANCELLED)
                    || toState.equals(SchedulingStates.SKIPPED);
            case SchedulingStates.READY_TO_SCHEDULE -> toState.equals(SchedulingStates.SCHEDULED)
                    || toState.equals(SchedulingStates.CANCELLED)
                    || toState.equals(SchedulingStates.SKIPPED);
            case SchedulingStates.SCHEDULED -> toState.equals(SchedulingStates.SNAPSHOT_CONFIRMED)
                    || toState.equals(SchedulingStates.SNAPSHOT_NOT_ADVANCED)
                    || toState.equals(SchedulingStates.CANCELLED);
            case SchedulingStates.SNAPSHOT_NOT_ADVANCED -> toState.equals(SchedulingStates.WAITING_SNAPSHOT)
                    || toState.equals(SchedulingStates.READY_TO_SCHEDULE)
                    || toState.equals(SchedulingStates.CANCELLED)
                    || toState.equals(SchedulingStates.SKIPPED);
            default -> false;
        };
    }

    /**
     * Mark task as waiting for snapshot evidence.
     */
    public void markWaitingForSnapshot(Long taskId, String reason) {
        transitionState(taskId, SchedulingStates.WAITING_SNAPSHOT, reason);
    }

    /**
     * Mark task as schedulable. This is a decision state, not executor readiness.
     */
    public void markSchedulable(Long taskId) {
        transitionState(taskId, SchedulingStates.READY_TO_SCHEDULE, null);
    }

    /**
     * Mark that Lakehouse Flow emitted the scheduling decision.
     */
    public void markScheduled(Long taskId) {
        transitionState(taskId, SchedulingStates.SCHEDULED, null);
    }

    /**
     * Mark that Lakehouse Flow emitted the scheduling decision and record the
     * target snapshot baseline captured immediately before scheduling.
     */
    public void markScheduled(Long taskId, String targetAssetKey, String baselineSnapshotId) {
        recordSchedulingBaseline(taskId, targetAssetKey, baselineSnapshotId);
        transitionState(taskId, SchedulingStates.SCHEDULED, null);
    }

    /**
     * Mark a task as skipped by a scheduling action.
     *
     * @param taskId task instance id
     * @param reason reason recorded for audit and operator visibility
     */
    public void markSkipped(Long taskId, String reason) {
        transitionState(taskId, SchedulingStates.SKIPPED, reason);
    }

    /**
     * Cancel a task scheduling instance.
     *
     * @param taskId task instance id
     * @param reason cancellation reason recorded on the task
     */
    public void cancel(Long taskId, String reason) {
        transitionState(taskId, SchedulingStates.CANCELLED, reason);
    }

    /**
     * Record a non-terminal snapshot confirmation check.
     *
     * The task remains SCHEDULED until target snapshot progress is confirmed or
     * the confirmation window expires.
     */
    public void recordSnapshotCheck(
            Long taskId,
            String targetAssetKey,
            String baselineSnapshotId,
            String observedSnapshotId,
            String waitingReason) {

        recordSnapshotEvidence(taskId, targetAssetKey, baselineSnapshotId, observedSnapshotId, waitingReason, true);
    }

    /**
     * Confirm that the target asset snapshot advanced after the baseline.
     */
    public void confirmSnapshotProgress(
            Long taskId,
            String targetAssetKey,
            String baselineSnapshotId,
            String observedSnapshotId) {

        recordSnapshotEvidence(taskId, targetAssetKey, baselineSnapshotId, observedSnapshotId, null, true);
        transitionState(taskId, SchedulingStates.SNAPSHOT_CONFIRMED, null);
    }

    /**
     * Record that the target snapshot has not advanced after the baseline.
     */
    public void markSnapshotNotAdvanced(
            Long taskId,
            String targetAssetKey,
            String baselineSnapshotId,
            String observedSnapshotId,
            String reason) {

        recordSnapshotEvidence(taskId, targetAssetKey, baselineSnapshotId, observedSnapshotId, reason, true);
        transitionState(taskId, SchedulingStates.SNAPSHOT_NOT_ADVANCED, reason);
    }

    /**
     * Capture the target snapshot baseline immediately before scheduling.
     *
     * @param taskId task instance id
     * @param targetAssetKey target asset used to confirm scheduling outcome
     * @param baselineSnapshotId snapshot observed before scheduling intent emission
     */
    private void recordSchedulingBaseline(
            Long taskId,
            String targetAssetKey,
            String baselineSnapshotId) {

        recordSnapshotEvidence(taskId, targetAssetKey, baselineSnapshotId, baselineSnapshotId, null, false);
    }

    /**
     * Persist target snapshot evidence on a task instance.
     *
     * @param taskId task instance id
     * @param targetAssetKey target asset used for confirmation
     * @param baselineSnapshotId snapshot captured before scheduling
     * @param observedSnapshotId latest snapshot observed during confirmation
     * @param waitingReason reason progress is still pending or failed
     * @param updateCheckTime whether to refresh the last snapshot check timestamp
     */
    private void recordSnapshotEvidence(
            Long taskId,
            String targetAssetKey,
            String baselineSnapshotId,
            String observedSnapshotId,
            String waitingReason,
            boolean updateCheckTime) {

        TaskInstance t = taskInstanceRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task instance not found: " + taskId));
        t.setTargetAssetKey(targetAssetKey);
        t.setBaselineSnapshotId(baselineSnapshotId);
        t.setObservedSnapshotId(observedSnapshotId);
        t.setWaitingReason(waitingReason);
        if (updateCheckTime) {
            t.setLastSnapshotCheckAt(LocalDateTime.now());
        }
        taskInstanceRepository.save(t);
    }

    /**
     * Get task instance
     */
    @Transactional(readOnly = true)
    public Optional<TaskInstance> getInstance(Long taskId) {
        return taskInstanceRepository.findById(taskId);
    }

    /**
     * Find tasks waiting for snapshot evidence.
     */
    @Transactional(readOnly = true)
    public List<TaskInstance> findWaitingForSnapshot() {
        return taskInstanceRepository.findWaitingForSnapshot();
    }

    /**
     * Find tasks ready to schedule.
     */
    @Transactional(readOnly = true)
    public List<TaskInstance> findSchedulableTasks() {
        return taskInstanceRepository.findDeliverableReadyTasks();
    }

    /**
     * Build instance key for idempotency
     */
    private String buildInstanceKey(Long workflowInstanceId, String taskCode) {
        return String.format("%d:%s", workflowInstanceId, taskCode);
    }
}
