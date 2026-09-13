package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests snapshot-driven progression through one immutable FlowPlan DAG.
 */
@ExtendWith(MockitoExtension.class)
class DagProgressionServiceTest {

    private static final LocalDateTime BIZ_DATE = LocalDate.of(2026, 9, 12).atStartOfDay();

    @Mock
    private TaskInstanceRepository taskInstanceRepository;

    @Mock
    private WorkflowInstanceRepository workflowInstanceRepository;

    @Mock
    private ScheduleNodeRepository scheduleNodeRepository;

    @Mock
    private FlowPlanVersionRepository flowPlanVersionRepository;

    @Mock
    private BackfillItemRepository backfillItemRepository;

    @Mock
    private BackfillProgressionService backfillProgressionService;

    @Mock
    private TaskInstanceService taskInstanceService;

    @Mock
    private WorkflowInstanceService workflowInstanceService;

    @Mock
    private FlowPlanConditionService flowPlanConditionService;

    @InjectMocks
    private DagProgressionService dagProgressionService;

    /**
     * Verify a downstream node is released after every direct upstream snapshot confirms.
     */
    @Test
    void onSnapshotConfirmedReleasesOnlyAfterEveryDirectParentConfirms() {
        TaskInstance left = task(71L, 61L, "left", SchedulingStates.SNAPSHOT_CONFIRMED);
        TaskInstance right = task(72L, 62L, "right", SchedulingStates.SNAPSHOT_CONFIRMED);
        TaskInstance downstream = task(73L, 63L, "join", SchedulingStates.WAITING_SNAPSHOT);
        ScheduleNode leftNode = node(61L, "left", List.of());
        ScheduleNode rightNode = node(62L, "right", List.of());
        ScheduleNode join = node(63L, "join", List.of("left", "right"));
        stubConfirmedProgression(
                left,
                List.of(left, right, downstream),
                List.of(leftNode, rightNode, join));
        when(flowPlanConditionService.evaluate(Map.of(), BIZ_DATE.toLocalDate()))
                .thenReturn(EvaluationResult.satisfied());

        dagProgressionService.onSnapshotConfirmed(71L);

        verify(taskInstanceService).markSchedulable(73L);
    }

    /**
     * Verify one present but not snapshot-confirmed parent keeps a join node waiting.
     */
    @Test
    void onSnapshotConfirmedDoesNotReleaseWhileAnotherParentHasNotAdvanced() {
        TaskInstance left = task(71L, 61L, "left", SchedulingStates.SNAPSHOT_CONFIRMED);
        TaskInstance right = task(72L, 62L, "right", SchedulingStates.SCHEDULED);
        TaskInstance downstream = task(73L, 63L, "join", SchedulingStates.WAITING_SNAPSHOT);
        ScheduleNode leftNode = node(61L, "left", List.of());
        ScheduleNode rightNode = node(62L, "right", List.of());
        ScheduleNode join = node(63L, "join", List.of("left", "right"));
        stubConfirmedProgression(
                left,
                List.of(left, right, downstream),
                List.of(leftNode, rightNode, join));

        dagProgressionService.onSnapshotConfirmed(71L);

        verify(taskInstanceService, never()).markSchedulable(73L);
    }

    /**
     * Verify one absent direct upstream task blocks downstream scheduling.
     */
    @Test
    void onSnapshotConfirmedDoesNotReleaseWhenDeclaredUpstreamIsMissing() {
        TaskInstance upstream = task(71L, 61L, "root", SchedulingStates.SNAPSHOT_CONFIRMED);
        TaskInstance downstream = task(73L, 63L, "join", SchedulingStates.WAITING_SNAPSHOT);
        ScheduleNode root = node(61L, "root", List.of());
        ScheduleNode missing = node(62L, "other", List.of());
        ScheduleNode join = node(63L, "join", List.of("root", "other"));
        stubConfirmedProgression(upstream, List.of(upstream, downstream), List.of(root, missing, join));

        dagProgressionService.onSnapshotConfirmed(71L);

        verify(taskInstanceService, never()).markSchedulable(73L);
    }

    /**
     * Verify an external asset event can release a node whose DAG parents already confirmed.
     */
    @Test
    void onAssetStateAdvancedReleasesExternallyGatedNode() {
        Map<String, Object> dependency = Map.of(
                "type", "SNAPSHOT_ADVANCED",
                "assetKey", "payments.dt=${bizDate}");
        TaskInstance upstream = task(71L, 61L, "root", SchedulingStates.SNAPSHOT_CONFIRMED);
        TaskInstance downstream = task(72L, 62L, "leaf", SchedulingStates.WAITING_SNAPSHOT);
        ScheduleNode root = node(61L, "root", List.of());
        ScheduleNode leaf = node(62L, "leaf", List.of("root"));
        leaf.setInputDependencySpecJson(dependency);
        FlowPlanVersion version = version();
        when(taskInstanceRepository.findWaitingForSnapshot()).thenReturn(List.of(downstream));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findById(62L)).thenReturn(Optional.of(leaf));
        when(flowPlanConditionService.referencesAsset(
                dependency,
                BIZ_DATE.toLocalDate(),
                "payments.dt=2026-09-12")).thenReturn(true);
        when(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(11L))
                .thenReturn(List.of(upstream, downstream));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(root, leaf));
        when(flowPlanConditionService.evaluate(dependency, BIZ_DATE.toLocalDate()))
                .thenReturn(EvaluationResult.satisfied());

        dagProgressionService.onAssetStateAdvanced("payments.dt=2026-09-12", BIZ_DATE);

        verify(taskInstanceService).markSchedulable(72L);
    }

    /**
     * Verify external asset events cannot bypass a backfill date concurrency queue.
     */
    @Test
    void onAssetStateAdvancedDoesNotReleaseConcurrencyQueuedStartNode() {
        Map<String, Object> dependency = Map.of(
                "type", "SNAPSHOT_ADVANCED",
                "assetKey", "orders.dt=${bizDate}");
        TaskInstance queued = task(71L, 61L, "root", SchedulingStates.WAITING_SNAPSHOT);
        ScheduleNode root = node(61L, "root", List.of());
        root.setInputDependencySpecJson(dependency);
        when(taskInstanceRepository.findWaitingForSnapshot()).thenReturn(List.of(queued));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version()));
        when(scheduleNodeRepository.findById(61L)).thenReturn(Optional.of(root));
        when(flowPlanConditionService.referencesAsset(
                dependency,
                BIZ_DATE.toLocalDate(),
                "orders.dt=2026-09-12")).thenReturn(true);
        when(backfillItemRepository.findByTaskInstanceId(71L))
                .thenReturn(Optional.of(BackfillItem.builder()
                        .taskInstanceId(71L)
                        .status(BackfillItemStatuses.WAITING_CONCURRENCY)
                        .build()));

        dagProgressionService.onAssetStateAdvanced("orders.dt=2026-09-12", BIZ_DATE);

        verify(taskInstanceService, never()).markSchedulable(71L);
        verify(taskInstanceRepository, never()).findByWorkflowInstanceIdOrderByCreatedAtAsc(11L);
    }

    /**
     * Verify a non-advancing target snapshot propagates to workflow evidence.
     */
    @Test
    void onSnapshotNotAdvancedMarksWorkflowOutcome() {
        TaskInstance task = task(71L, 61L, "root", SchedulingStates.SNAPSHOT_NOT_ADVANCED);
        when(taskInstanceRepository.findById(71L)).thenReturn(Optional.of(task));
        when(workflowInstanceRepository.findById(11L))
                .thenReturn(Optional.of(WorkflowInstance.builder()
                        .id(11L)
                        .state(SchedulingStates.SCHEDULED)
                        .build()));
        when(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(11L))
                .thenReturn(List.of(task));

        dagProgressionService.onSnapshotNotAdvanced(71L);

        verify(workflowInstanceService).markSnapshotNotAdvanced(11L);
    }

    /**
     * Stub common state used when one task has just confirmed its target snapshot.
     *
     * @param confirmedTask confirmed task
     * @param workflowTasks tasks in the same DAG instance
     * @param nodes immutable graph nodes
     */
    private void stubConfirmedProgression(
            TaskInstance confirmedTask,
            List<TaskInstance> workflowTasks,
            List<ScheduleNode> nodes) {
        when(taskInstanceRepository.findById(confirmedTask.getId())).thenReturn(Optional.of(confirmedTask));
        when(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(11L)).thenReturn(workflowTasks);
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L)).thenReturn(nodes);
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version()));
        when(workflowInstanceRepository.findById(11L))
                .thenReturn(Optional.of(WorkflowInstance.builder()
                        .id(11L)
                        .state(SchedulingStates.SCHEDULED)
                        .build()));
    }

    /**
     * Build a task fixture anchored to one graph node.
     *
     * @param id task id
     * @param nodeId schedule node id
     * @param code node code
     * @param state scheduling state
     * @return task fixture
     */
    private TaskInstance task(Long id, Long nodeId, String code, String state) {
        return TaskInstance.builder()
                .id(id)
                .workflowInstanceId(11L)
                .taskCode(code)
                .flowPlanVersionId(51L)
                .scheduleNodeId(nodeId)
                .bizDate(BIZ_DATE)
                .state(state)
                .build();
    }

    /**
     * Build a graph node fixture.
     *
     * @param id node id
     * @param code node code
     * @param dependencies direct upstream codes
     * @return schedule node fixture
     */
    private ScheduleNode node(Long id, String code, List<String> dependencies) {
        return ScheduleNode.builder()
                .id(id)
                .flowPlanVersionId(51L)
                .nodeCode(code)
                .dependsOnNodes(dependencies)
                .inputDependencySpecJson(Map.of())
                .build();
    }

    /**
     * Build an immutable plan version fixture.
     *
     * @return plan version fixture
     */
    private FlowPlanVersion version() {
        return FlowPlanVersion.builder()
                .id(51L)
                .dependencySpecJson(Map.of())
                .build();
    }
}
