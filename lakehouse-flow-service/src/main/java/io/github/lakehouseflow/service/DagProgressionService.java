package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Releases DAG nodes from scheduler-side snapshot evidence.
 *
 * A downstream batch node becomes deliverable only after every direct parent
 * supplies evidence. Batch and action-owned stream parents use same-instance
 * confirmed tasks; ordinary stream parents use their output AssetState.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class DagProgressionService {

    private final TaskInstanceRepository taskInstanceRepository;
    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final BackfillProgressionService backfillProgressionService;
    private final TaskInstanceService taskInstanceService;
    private final WorkflowInstanceService workflowInstanceService;
    private final InputSnapshotEvidenceService inputSnapshotEvidenceService;

    /**
     * Re-evaluate waiting DAG nodes whose external asset gate references an
     * asset that just advanced.
     *
     * @param assetKey changed asset key
     * @param bizDate changed asset business time
     */
    public void onAssetStateAdvanced(String assetKey, LocalDateTime bizDate) {
        if (assetKey == null || bizDate == null) {
            return;
        }
        for (TaskInstance task : taskInstanceRepository.findWaitingForSnapshot()) {
            if (task.getFlowPlanVersionId() == null
                    || task.getScheduleNodeId() == null
                    || task.getBizDate() == null
                    || !task.getBizDate().toLocalDate().equals(bizDate.toLocalDate())) {
                continue;
            }
            releaseIfEligible(task);
        }
    }

    /**
     * Re-evaluate every waiting batch task in one newly created scheduling instance.
     *
     * <p>This closes the case where a streaming parent had already produced its date-scoped
     * snapshot before the workflow wrapper was created, so no later event is required to wake it.
     *
     * @param workflowInstanceId workflow scheduling instance id
     */
    public void releaseEligibleTasksForWorkflow(Long workflowInstanceId) {
        if (workflowInstanceId == null) {
            return;
        }
        taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(workflowInstanceId).stream()
                .filter(task -> SchedulingStates.WAITING_SNAPSHOT.equals(task.getState()))
                .forEach(this::releaseIfEligible);
    }

    /**
     * Record a confirmed node and release direct downstream nodes whose DAG inputs are confirmed.
     *
     * @param taskInstanceId snapshot-confirmed task scheduling instance
     */
    public void onSnapshotConfirmed(Long taskInstanceId) {
        TaskInstance confirmedTask = getTask(taskInstanceId);
        if (!SchedulingStates.SNAPSHOT_CONFIRMED.equals(confirmedTask.getState())) {
            throw new IllegalStateException("Task is not snapshot-confirmed: " + taskInstanceId);
        }

        Optional<BackfillItem> confirmedItem = backfillItemRepository.findByTaskInstanceId(taskInstanceId);
        confirmedItem.ifPresent(item -> updateItemStatus(item, BackfillItemStatuses.SNAPSHOT_CONFIRMED));
        releaseEligibleTasks(confirmedTask);
        updateWorkflowOutcome(confirmedTask.getWorkflowInstanceId());
        confirmedItem.ifPresent(item -> backfillProgressionService.refreshBatch(item.getBackfillBatchId()));
    }

    /**
     * Record that a delivered node did not advance its target snapshot.
     *
     * @param taskInstanceId task whose confirmation window expired
     */
    public void onSnapshotNotAdvanced(Long taskInstanceId) {
        TaskInstance task = getTask(taskInstanceId);
        if (!SchedulingStates.SNAPSHOT_NOT_ADVANCED.equals(task.getState())) {
            throw new IllegalStateException("Task has not reached SNAPSHOT_NOT_ADVANCED: " + taskInstanceId);
        }
        Optional<BackfillItem> failedItem = backfillItemRepository.findByTaskInstanceId(taskInstanceId);
        failedItem.ifPresent(item -> updateItemStatus(item, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED));
        updateWorkflowOutcome(task.getWorkflowInstanceId());
        failedItem.ifPresent(item -> backfillProgressionService.refreshBatch(item.getBackfillBatchId()));
    }

    /**
     * Release waiting tasks after all direct upstream node snapshots are confirmed.
     *
     * @param confirmedTask task that just supplied new snapshot evidence
     */
    private void releaseEligibleTasks(TaskInstance confirmedTask) {
        if (confirmedTask.getFlowPlanVersionId() == null) {
            return;
        }

        for (TaskInstance candidate : taskInstanceRepository
                .findByWorkflowInstanceIdOrderByCreatedAtAsc(confirmedTask.getWorkflowInstanceId())) {
            if (!SchedulingStates.WAITING_SNAPSHOT.equals(candidate.getState())) {
                continue;
            }
            releaseIfEligible(candidate);
        }
    }

    /**
     * Release one waiting node when both internal DAG and external asset gates pass.
     *
     * @param candidate waiting task scheduling instance
     */
    private void releaseIfEligible(TaskInstance candidate) {
        if (isWaitingForConcurrency(candidate.getId())) {
            return;
        }
        InputSnapshotEvidenceService.InputEvidenceEvaluation evaluation =
                inputSnapshotEvidenceService.evaluate(candidate);
        if (evaluation.satisfied()) {
            taskInstanceService.markSchedulable(candidate.getId());
            backfillItemRepository.findByTaskInstanceId(candidate.getId())
                    .ifPresent(item -> updateItemStatus(item, BackfillItemStatuses.INTENT_READY));
        }
    }

    /**
     * Check whether a backfill start node is waiting for date-level admission.
     *
     * External asset events and DAG confirmations must not bypass this queue.
     *
     * @param taskInstanceId candidate task scheduling instance
     * @return true when the owning backfill item is concurrency-queued
     */
    private boolean isWaitingForConcurrency(Long taskInstanceId) {
        return backfillItemRepository.findByTaskInstanceId(taskInstanceId)
                .map(BackfillItem::getStatus)
                .filter(BackfillItemStatuses.WAITING_CONCURRENCY::equals)
                .isPresent();
    }

    /**
     * Aggregate node snapshot outcomes into the workflow scheduling record.
     *
     * @param workflowInstanceId workflow to update
     */
    private void updateWorkflowOutcome(Long workflowInstanceId) {
        WorkflowInstance workflow = workflowInstanceRepository.findById(workflowInstanceId)
                .orElseThrow(() -> new IllegalStateException("Workflow instance not found: " + workflowInstanceId));
        if (!SchedulingStates.SCHEDULED.equals(workflow.getState())) {
            return;
        }
        List<TaskInstance> tasks = taskInstanceRepository
                .findByWorkflowInstanceIdOrderByCreatedAtAsc(workflowInstanceId);
        if (!tasks.isEmpty() && tasks.stream()
                .allMatch(task -> SchedulingStates.SNAPSHOT_CONFIRMED.equals(task.getState()))) {
            workflowInstanceService.confirmSnapshotProgress(workflowInstanceId);
        } else if (tasks.stream().anyMatch(task -> SchedulingStates.SNAPSHOT_NOT_ADVANCED.equals(task.getState()))) {
            workflowInstanceService.markSnapshotNotAdvanced(workflowInstanceId);
        }
    }

    /**
     * Persist a scheduler-side backfill item transition.
     *
     * @param item item to update
     * @param status new scheduler-side status
     */
    private void updateItemStatus(BackfillItem item, String status) {
        item.setStatus(status);
        backfillItemRepository.save(item);
    }

    /**
     * Load a task or fail with a scheduling-domain error.
     *
     * @param taskInstanceId task id
     * @return task instance
     */
    private TaskInstance getTask(Long taskInstanceId) {
        return taskInstanceRepository.findById(taskInstanceId)
                .orElseThrow(() -> new IllegalArgumentException("Task instance not found: " + taskInstanceId));
    }
}
