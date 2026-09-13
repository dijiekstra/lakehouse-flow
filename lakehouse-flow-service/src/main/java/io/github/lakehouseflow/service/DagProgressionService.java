package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Releases DAG nodes from scheduler-side snapshot evidence.
 *
 * A downstream node becomes deliverable only after every direct upstream node
 * represented in the same scheduling instance is snapshot-confirmed. A missing
 * upstream instance remains a blocking dependency instead of being assumed ready.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class DagProgressionService {

    private final TaskInstanceRepository taskInstanceRepository;
    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final ScheduleNodeRepository scheduleNodeRepository;
    private final FlowPlanVersionRepository flowPlanVersionRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final BackfillProgressionService backfillProgressionService;
    private final TaskInstanceService taskInstanceService;
    private final WorkflowInstanceService workflowInstanceService;
    private final FlowPlanConditionService flowPlanConditionService;

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
            FlowPlanVersion version = flowPlanVersionRepository.findById(task.getFlowPlanVersionId())
                    .orElseThrow(() -> new IllegalStateException(
                            "FlowPlanVersion not found: " + task.getFlowPlanVersionId()));
            ScheduleNode node = scheduleNodeRepository.findById(task.getScheduleNodeId())
                    .orElseThrow(() -> new IllegalStateException(
                            "ScheduleNode not found: " + task.getScheduleNodeId()));
            if (flowPlanConditionService.referencesAsset(
                    effectiveDependencySpec(version, node),
                    task.getBizDate().toLocalDate(),
                    assetKey)) {
                releaseIfEligible(task, version, node);
            }
        }
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

        List<TaskInstance> workflowTasks = taskInstanceRepository
                .findByWorkflowInstanceIdOrderByCreatedAtAsc(confirmedTask.getWorkflowInstanceId());
        List<ScheduleNode> nodes = scheduleNodeRepository
                .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(confirmedTask.getFlowPlanVersionId());
        FlowPlanVersion version = flowPlanVersionRepository.findById(confirmedTask.getFlowPlanVersionId())
                .orElseThrow(() -> new IllegalStateException(
                        "FlowPlanVersion not found: " + confirmedTask.getFlowPlanVersionId()));
        Map<Long, ScheduleNode> nodesById = nodes.stream()
                .collect(Collectors.toMap(ScheduleNode::getId, Function.identity()));
        Map<String, TaskInstance> tasksByNodeCode = workflowTasks.stream()
                .filter(task -> task.getScheduleNodeId() != null)
                .filter(task -> nodesById.containsKey(task.getScheduleNodeId()))
                .collect(Collectors.toMap(
                        task -> nodesById.get(task.getScheduleNodeId()).getNodeCode(),
                        Function.identity()));

        for (TaskInstance candidate : workflowTasks) {
            if (!SchedulingStates.WAITING_SNAPSHOT.equals(candidate.getState())) {
                continue;
            }
            if (isWaitingForConcurrency(candidate.getId())) {
                continue;
            }
            ScheduleNode node = nodesById.get(candidate.getScheduleNodeId());
            if (node != null
                    && dependenciesConfirmed(node, tasksByNodeCode)
                    && externalDependenciesConfirmed(version, node, candidate)) {
                taskInstanceService.markSchedulable(candidate.getId());
                backfillItemRepository.findByTaskInstanceId(candidate.getId())
                        .ifPresent(item -> updateItemStatus(item, BackfillItemStatuses.INTENT_READY));
            }
        }
    }

    /**
     * Release one waiting node when both internal DAG and external asset gates pass.
     *
     * @param candidate waiting task scheduling instance
     * @param version immutable FlowPlan version
     * @param node candidate node definition
     */
    private void releaseIfEligible(TaskInstance candidate, FlowPlanVersion version, ScheduleNode node) {
        if (isWaitingForConcurrency(candidate.getId())) {
            return;
        }
        List<TaskInstance> workflowTasks = taskInstanceRepository
                .findByWorkflowInstanceIdOrderByCreatedAtAsc(candidate.getWorkflowInstanceId());
        List<ScheduleNode> nodes = scheduleNodeRepository
                .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(version.getId());
        Map<Long, ScheduleNode> nodesById = nodes.stream()
                .collect(Collectors.toMap(ScheduleNode::getId, Function.identity()));
        Map<String, TaskInstance> tasksByNodeCode = workflowTasks.stream()
                .filter(task -> task.getScheduleNodeId() != null)
                .filter(task -> nodesById.containsKey(task.getScheduleNodeId()))
                .collect(Collectors.toMap(
                        task -> nodesById.get(task.getScheduleNodeId()).getNodeCode(),
                        Function.identity()));
        if (dependenciesConfirmed(node, tasksByNodeCode)
                && externalDependenciesConfirmed(version, node, candidate)) {
            taskInstanceService.markSchedulable(candidate.getId());
            backfillItemRepository.findByTaskInstanceId(candidate.getId())
                    .ifPresent(item -> updateItemStatus(item, BackfillItemStatuses.INTENT_READY));
        }
    }

    /**
     * Check node-level external input gates using current AssetState evidence.
     *
     * @param version plan version supplying default dependencies
     * @param node candidate node
     * @param task date-scoped task scheduling instance
     * @return true when all configured external gates allow release
     */
    private boolean externalDependenciesConfirmed(
            FlowPlanVersion version,
            ScheduleNode node,
            TaskInstance task) {
        if (task.getBizDate() == null) {
            return false;
        }
        return Boolean.TRUE.equals(flowPlanConditionService.evaluate(
                effectiveDependencySpec(version, node),
                task.getBizDate().toLocalDate()).getSatisfied());
    }

    /**
     * Resolve node-level dependencies with version defaults.
     *
     * @param version FlowPlan version
     * @param node schedule node
     * @return effective dependency specification
     */
    private Map<String, Object> effectiveDependencySpec(FlowPlanVersion version, ScheduleNode node) {
        Map<String, Object> nodeSpec = node.getInputDependencySpecJson();
        return nodeSpec == null || nodeSpec.isEmpty() ? version.getDependencySpecJson() : nodeSpec;
    }

    /**
     * Check all direct upstream nodes inside the immutable FlowPlan graph.
     *
     * @param node candidate downstream node
     * @param tasksByNodeCode tasks present in the same scheduling instance
     * @return true only when every declared upstream has a confirmed task
     */
    private boolean dependenciesConfirmed(ScheduleNode node, Map<String, TaskInstance> tasksByNodeCode) {
        List<String> dependencies = node.getDependsOnNodes() == null ? List.of() : node.getDependsOnNodes();
        return dependencies.stream().allMatch(code -> {
            TaskInstance upstream = tasksByNodeCode.get(code);
            return upstream != null && SchedulingStates.SNAPSHOT_CONFIRMED.equals(upstream.getState());
        });
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
