package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillCascadePolicies;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.common.BackfillRecoveryStrategies;
import io.github.lakehouseflow.common.BackfillScopeTypes;
import io.github.lakehouseflow.common.BackfillSkipPolicies;
import io.github.lakehouseflow.common.FlowPlanVersionStatuses;
import io.github.lakehouseflow.common.SchedulingActionStatuses;
import io.github.lakehouseflow.common.SchedulingActionTypes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.SchedulingActionRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.SchedulingAction;
import io.github.lakehouseflow.model.SchedulingActionResult;
import io.github.lakehouseflow.model.SnapshotConfirmationResult;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests scheduling-side actions without invoking external execution.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingActionServiceTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 0, 0);

    @Mock
    private SchedulingActionRepository schedulingActionRepository;

    @Mock
    private BackfillBatchRepository backfillBatchRepository;

    @Mock
    private BackfillItemRepository backfillItemRepository;

    @Mock
    private FlowPlanVersionRepository flowPlanVersionRepository;

    @Mock
    private ScheduleNodeRepository scheduleNodeRepository;

    @Mock
    private WorkflowInstanceRepository workflowInstanceRepository;

    @Mock
    private TaskInstanceRepository taskInstanceRepository;

    @Mock
    private WorkflowInstanceService workflowInstanceService;

    @Mock
    private TaskInstanceService taskInstanceService;

    @Mock
    private SnapshotConfirmationService snapshotConfirmationService;

    @Mock
    private BackfillProgressionService backfillProgressionService;

    @Spy
    private FlowPlanGraphService flowPlanGraphService = new FlowPlanGraphService();

    @Spy
    private SchedulingTemplateResolver schedulingTemplateResolver = new SchedulingTemplateResolver();

    @InjectMocks
    private SchedulingActionService schedulingActionService;

    /**
     * Verify rerun emits a new workflow scheduling decision without executing work.
     */
    @Test
    void rerunWorkflowInstanceEmitsNewSchedulingDecision() {
        when(schedulingActionRepository.findByActionKey("rerun-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        WorkflowInstance source = workflow(11L, SchedulingStates.SNAPSHOT_NOT_ADVANCED);
        TaskInstance sourceTask = task(22L);
        sourceTask.setTargetAssetKey("paimon.prod.dwd_orders");
        when(workflowInstanceRepository.findById(11L)).thenReturn(Optional.of(source));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(2),
                eq(BIZ_DATE),
                eq("RERUN"),
                eq("rerun-1"),
                any(),
                isNull()))
                .thenReturn(workflow(21L, SchedulingStates.CREATED));
        when(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(11L))
                .thenReturn(List.of(sourceTask));
        when(taskInstanceService.createInstance(
                21L,
                "node.dwd_orders",
                1,
                BIZ_DATE,
                "paimon.prod.dwd_orders",
                null,
                null)).thenReturn(task(23L));

        SchedulingActionResult result = schedulingActionService.rerunWorkflowInstance(
                11L,
                "rerun-1",
                "alice",
                "operator retry");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(21L), result.workflowInstanceIds());
        assertEquals(23L, result.taskInstanceId());
        verify(taskInstanceService).markSchedulableAsActionEntry(23L);
        verify(workflowInstanceService).markSchedulable(21L);
        ArgumentCaptor<SchedulingAction> actionCaptor = ArgumentCaptor.forClass(SchedulingAction.class);
        verify(schedulingActionRepository, times(2)).save(actionCaptor.capture());
        SchedulingAction savedAction = actionCaptor.getAllValues().get(1);
        assertEquals("flow.orders", savedAction.getWorkflowCode());
        assertEquals(2, savedAction.getWorkflowVersion());
        assertEquals(BIZ_DATE.toLocalDate(), savedAction.getBizDateStart());
    }

    /**
     * Verify task rerun emits a fresh downstream-consumable task intent.
     */
    @Test
    void rerunTaskInstanceEmitsNewTaskSchedulingIntent() {
        TaskInstance sourceTask = task(22L);
        sourceTask.setTargetAssetKey("paimon.prod.dwd_orders");
        sourceTask.setFlowPlanVersionId(51L);
        sourceTask.setScheduleNodeId(61L);
        WorkflowInstance sourceWorkflow = workflow(11L, SchedulingStates.SNAPSHOT_NOT_ADVANCED);
        WorkflowInstance rerunWorkflow = workflow(21L, SchedulingStates.CREATED);
        TaskInstance rerunTask = task(23L);
        when(schedulingActionRepository.findByActionKey("rerun-task-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(sourceTask));
        when(workflowInstanceRepository.findById(11L)).thenReturn(Optional.of(sourceWorkflow));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(2),
                eq(BIZ_DATE),
                eq("RERUN_TASK"),
                eq("rerun-task-1"),
                any(),
                eq(51L)))
                .thenReturn(rerunWorkflow);
        when(taskInstanceService.createInstance(
                eq(21L),
                eq("node.dwd_orders"),
                eq(1),
                eq(BIZ_DATE),
                eq("paimon.prod.dwd_orders"),
                eq(51L),
                eq(61L)))
                .thenReturn(rerunTask);

        SchedulingActionResult result = schedulingActionService.rerunTaskInstance(
                22L,
                "rerun-task-1",
                "alice",
                "rerun only this node");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(21L), result.workflowInstanceIds());
        assertEquals(23L, result.taskInstanceId());
        ArgumentCaptor<SchedulingAction> actionCaptor = ArgumentCaptor.forClass(SchedulingAction.class);
        verify(schedulingActionRepository, times(2)).save(actionCaptor.capture());
        SchedulingAction savedAction = actionCaptor.getAllValues().get(1);
        assertEquals("flow.orders", savedAction.getWorkflowCode());
        assertEquals(51L, savedAction.getFlowPlanVersionId());
        assertEquals(61L, savedAction.getScheduleNodeId());
        assertEquals("paimon.prod.dwd_orders", savedAction.getTargetAssetKey());
        verify(taskInstanceService).markSchedulableAsActionEntry(23L);
        verify(workflowInstanceService).markSchedulable(21L);
    }

    /**
     * Verify missing task rerun requests are rejected as scheduler validation failures.
     */
    @Test
    void rerunTaskInstanceRejectsMissingTask() {
        when(schedulingActionRepository.findByActionKey("rerun-task-missing")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(taskInstanceRepository.findById(404L)).thenReturn(Optional.empty());

        SchedulingActionResult result = schedulingActionService.rerunTaskInstance(
                404L,
                "rerun-task-missing",
                "alice",
                "missing task");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("Task instance not found"));
    }

    /**
     * Verify published FlowPlan node rerun emits a fresh node scheduling intent.
     */
    @Test
    void rerunScheduleNodeEmitsNodeSchedulingIntent() {
        LocalDate bizDate = LocalDate.of(2026, 9, 12);
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode node = scheduleNode(61L);
        WorkflowInstance rerunWorkflow = workflow(31L, SchedulingStates.CREATED);
        TaskInstance rerunTask = task(32L);
        when(schedulingActionRepository.findByActionKey("rerun-node-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(node));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(4),
                eq(BIZ_DATE),
                eq("RERUN_TASK"),
                eq("rerun-node-1"),
                any(),
                eq(51L)))
                .thenReturn(rerunWorkflow);
        when(taskInstanceService.createInstance(
                eq(31L),
                eq("node.dwd_orders"),
                eq(4),
                eq(BIZ_DATE),
                eq("paimon.prod.dwd_orders"),
                eq(51L),
                eq(61L)))
                .thenReturn(rerunTask);

        SchedulingActionResult result = schedulingActionService.rerunScheduleNode(
                51L,
                "node.dwd_orders",
                bizDate,
                "rerun-node-1",
                "alice",
                "rerun published node");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(31L), result.workflowInstanceIds());
        assertEquals(32L, result.taskInstanceId());
        verify(taskInstanceService).markSchedulableAsActionEntry(32L);
        verify(workflowInstanceService).markSchedulable(31L);
    }

    /**
     * Verify draft FlowPlan versions cannot emit node rerun intents.
     */
    @Test
    void rerunScheduleNodeRejectsDraftVersion() {
        when(schedulingActionRepository.findByActionKey("rerun-node-draft")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L))
                .thenReturn(Optional.of(flowPlanVersion(51L, FlowPlanVersionStatuses.DRAFT)));

        SchedulingActionResult result = schedulingActionService.rerunScheduleNode(
                51L,
                "node.dwd_orders",
                LocalDate.of(2026, 9, 12),
                "rerun-node-draft",
                "alice",
                "should reject");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("published"));
        verify(scheduleNodeRepository, never()).findByFlowPlanVersionIdAndNodeCode(any(), any());
    }

    /**
     * Verify backfill creates one scheduling decision per business date.
     */
    @Test
    void backfillWorkflowCreatesInclusiveBusinessDateRange() {
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        version.setVersion(3);
        ScheduleNode node = scheduleNode(61L);
        TaskInstance firstTask = task(71L);
        TaskInstance secondTask = task(72L);
        when(schedulingActionRepository.findByActionKey("backfill-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        stubBackfillBatchSave(80L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findByFlowCodeAndVersion("flow.orders", 3))
                .thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(node));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(3),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("BACKFILL"),
                eq("backfill-1:2026-09-10"),
                any(),
                eq(51L)))
                .thenReturn(workflow(31L, SchedulingStates.CREATED));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(3),
                eq(LocalDate.of(2026, 9, 11).atStartOfDay()),
                eq("BACKFILL"),
                eq("backfill-1:2026-09-11"),
                any(),
                eq(51L)))
                .thenReturn(workflow(32L, SchedulingStates.CREATED));
        when(taskInstanceService.createInstance(
                31L, "node.dwd_orders", 3, LocalDate.of(2026, 9, 10).atStartOfDay(),
                "paimon.prod.dwd_orders", 51L, 61L)).thenReturn(firstTask);
        when(taskInstanceService.createInstance(
                32L, "node.dwd_orders", 3, LocalDate.of(2026, 9, 11).atStartOfDay(),
                "paimon.prod.dwd_orders", 51L, 61L)).thenReturn(secondTask);

        SchedulingActionResult result = schedulingActionService.backfillWorkflow(
                "flow.orders",
                3,
                LocalDate.of(2026, 9, 10),
                LocalDate.of(2026, 9, 11),
                "backfill-1",
                "alice",
                "补数两天");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(31L, 32L), result.workflowInstanceIds());
        assertEquals(71L, result.taskInstanceId());
        verify(taskInstanceService).markSchedulableAsActionEntry(71L);
        verify(taskInstanceService).markSchedulableAsActionEntry(72L);
        verify(workflowInstanceService).markSchedulable(31L);
        verify(workflowInstanceService).markSchedulable(32L);

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch batch = batchCaptor.getAllValues().get(1);
        assertEquals(BackfillScopeTypes.FULL_FLOW, batch.getScopeType());
        assertEquals(List.of("node.dwd_orders"), batch.getEntryNodeCodes());
        assertEquals(List.of("node.dwd_orders"), batch.getSelectedNodeCodes());
        assertNull(batch.getStartNodeCode());
        assertNull(batch.getCascadePolicy());
        assertEquals(2, batch.getProducedWorkflowCount());
        assertEquals(2, batch.getTotalItemCount());
        assertBackfillItemsSaved(2, 2, 0);
    }

    /**
     * Verify complete-Flow serial backfill admits all graph roots as one date slot.
     */
    @Test
    void backfillWorkflowUsesUnifiedBatchForMultipleRoots() {
        LocalDate dayOne = LocalDate.of(2026, 9, 10);
        LocalDate dayTwo = LocalDate.of(2026, 9, 11);
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        version.setVersion(3);
        ScheduleNode ordersRoot = scheduleNode(
                61L, "node.ods_orders", List.of(), "paimon.prod.ods_orders.dt=${bizDate}");
        ScheduleNode paymentsRoot = scheduleNode(
                62L, "node.ods_payments", List.of(), "paimon.prod.ods_payments.dt=${bizDate}");
        ScheduleNode joinedNode = scheduleNode(
                63L,
                "node.dwd_order_payments",
                List.of("node.ods_orders", "node.ods_payments"),
                "paimon.prod.dwd_order_payments.dt=${bizDate}");
        when(schedulingActionRepository.findByActionKey("backfill-full-serial")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        stubBackfillBatchSave(80L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findByFlowCodeAndVersion("flow.orders", 3))
                .thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(ordersRoot, paymentsRoot, joinedNode));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(3), eq(dayOne.atStartOfDay()), eq("BACKFILL"),
                eq("backfill-full-serial:2026-09-10"), any(), eq(51L)))
                .thenReturn(workflow(31L, SchedulingStates.CREATED));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(3), eq(dayTwo.atStartOfDay()), eq("BACKFILL"),
                eq("backfill-full-serial:2026-09-11"), any(), eq(51L)))
                .thenReturn(workflow(32L, SchedulingStates.CREATED));
        when(taskInstanceService.createInstance(
                anyLong(), any(), eq(3), any(), any(), eq(51L), anyLong()))
                .thenReturn(task(71L), task(72L), task(73L), task(74L), task(75L), task(76L));

        SchedulingActionResult result = schedulingActionService.backfillWorkflow(
                "flow.orders",
                3,
                dayOne,
                dayTwo,
                BackfillProgressionModes.SERIAL,
                null,
                BackfillSkipPolicies.NONE,
                "backfill-full-serial",
                "alice",
                "repair complete flow");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(31L, 32L), result.workflowInstanceIds());
        verify(taskInstanceService).markSchedulableAsActionEntry(71L);
        verify(taskInstanceService).markSchedulableAsActionEntry(72L);
        verify(taskInstanceService).markWaitingForSnapshot(eq(73L), contains("node.ods_orders,node.ods_payments"));
        verify(taskInstanceService).markWaitingForSnapshot(eq(74L), contains("Waiting for backfill date slot"));
        verify(taskInstanceService).markWaitingForSnapshot(eq(75L), contains("Waiting for backfill date slot"));
        verify(taskInstanceService).markWaitingForSnapshot(eq(76L), contains("node.ods_orders,node.ods_payments"));

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch batch = batchCaptor.getAllValues().get(1);
        assertEquals(BackfillScopeTypes.FULL_FLOW, batch.getScopeType());
        assertEquals(List.of("node.ods_orders", "node.ods_payments"), batch.getEntryNodeCodes());
        assertEquals(
                List.of("node.ods_orders", "node.ods_payments", "node.dwd_order_payments"),
                batch.getSelectedNodeCodes());
        assertEquals(BackfillProgressionModes.SERIAL, batch.getProgressionMode());

        ArgumentCaptor<BackfillItem> itemCaptor = ArgumentCaptor.forClass(BackfillItem.class);
        verify(backfillItemRepository, times(6)).save(itemCaptor.capture());
        assertEquals(2, itemCaptor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.INTENT_READY.equals(item.getStatus()))
                .count());
        assertEquals(2, itemCaptor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.WAITING_CONCURRENCY.equals(item.getStatus()))
                .count());
        assertEquals(2, itemCaptor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.WAITING_DEPENDENCY.equals(item.getStatus()))
                .count());
    }

    /**
     * Verify node backfill expands date range and direct downstream scope into task intents.
     */
    @Test
    void backfillScheduleNodeCreatesDateScopedDirectDownstreamTaskIntents() {
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode omittedUpstream = scheduleNode(
                60L,
                "node.ods_orders",
                List.of(),
                "paimon.prod.ods_orders");
        ScheduleNode startNode = scheduleNode(
                61L,
                "node.dwd_orders",
                List.of("node.ods_orders"),
                "paimon.prod.dwd_orders");
        ScheduleNode directDownstream = scheduleNode(
                62L,
                "node.dm_orders",
                List.of("node.dwd_orders"),
                "paimon.prod.dm_orders");
        ScheduleNode transitiveDownstream = scheduleNode(
                63L,
                "node.ads_orders",
                List.of("node.dm_orders"),
                "paimon.prod.ads_orders");
        WorkflowInstance dayOneWorkflow = workflow(41L, SchedulingStates.CREATED);
        WorkflowInstance dayTwoWorkflow = workflow(42L, SchedulingStates.CREATED);
        TaskInstance taskOne = task(71L);
        TaskInstance taskTwo = task(72L);
        TaskInstance taskThree = task(73L);
        TaskInstance taskFour = task(74L);
        when(schedulingActionRepository.findByActionKey("backfill-node-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        stubBackfillBatchSave(81L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(omittedUpstream, startNode, directDownstream, transitiveDownstream));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("BACKFILL"),
                eq("backfill-node-1:2026-09-10"),
                any(),
                eq(51L)))
                .thenReturn(dayOneWorkflow);
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 11).atStartOfDay()),
                eq("BACKFILL"),
                eq("backfill-node-1:2026-09-11"),
                any(),
                eq(51L)))
                .thenReturn(dayTwoWorkflow);
        when(taskInstanceService.createInstance(
                eq(41L),
                eq("node.dwd_orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("paimon.prod.dwd_orders"),
                eq(51L),
                eq(61L)))
                .thenReturn(taskOne);
        when(taskInstanceService.createInstance(
                eq(41L),
                eq("node.dm_orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("paimon.prod.dm_orders"),
                eq(51L),
                eq(62L)))
                .thenReturn(taskTwo);
        when(taskInstanceService.createInstance(
                eq(42L),
                eq("node.dwd_orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 11).atStartOfDay()),
                eq("paimon.prod.dwd_orders"),
                eq(51L),
                eq(61L)))
                .thenReturn(taskThree);
        when(taskInstanceService.createInstance(
                eq(42L),
                eq("node.dm_orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 11).atStartOfDay()),
                eq("paimon.prod.dm_orders"),
                eq(51L),
                eq(62L)))
                .thenReturn(taskFour);

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                LocalDate.of(2026, 9, 10),
                LocalDate.of(2026, 9, 11),
                "DIRECT_DOWNSTREAM",
                "PARALLEL",
                null,
                "backfill-node-1",
                "alice",
                "repair partitions");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(41L, 42L), result.workflowInstanceIds());
        assertEquals(71L, result.taskInstanceId());
        assertTrue(result.message().contains("taskIntents=4"));
        verify(taskInstanceService, times(2)).markSchedulableAsActionEntry(anyLong());
        verify(taskInstanceService, times(2)).markWaitingForSnapshot(anyLong(), any());
        assertExpandedBatch(2, 4);
        assertBackfillItemsSaved(4, 2, 2);
        verify(taskInstanceService, never()).createInstance(
                anyLong(),
                eq("node.ods_orders"),
                eq(4),
                any(),
                eq("paimon.prod.ods_orders"),
                eq(51L),
                eq(60L));
        verify(taskInstanceService, never()).createInstance(
                anyLong(),
                eq("node.ads_orders"),
                eq(4),
                any(),
                eq("paimon.prod.ads_orders"),
                eq(51L),
                eq(63L));
    }

    /**
     * Verify serial node backfill admits only the first business date initially.
     */
    @Test
    void backfillScheduleNodeSerialModeQueuesLaterBusinessDates() {
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode startNode = scheduleNode(
                61L,
                "node.dwd_orders",
                List.of(),
                "paimon.prod.dwd_orders.dt=${bizDate}");
        WorkflowInstance dayOneWorkflow = workflow(41L, SchedulingStates.CREATED);
        WorkflowInstance dayTwoWorkflow = workflow(42L, SchedulingStates.CREATED);
        TaskInstance dayOneTask = task(71L);
        TaskInstance dayTwoTask = task(72L);
        when(schedulingActionRepository.findByActionKey("backfill-node-serial")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        stubBackfillBatchSave(81L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(startNode));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(4), eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("BACKFILL"), eq("backfill-node-serial:2026-09-10"), any(), eq(51L)))
                .thenReturn(dayOneWorkflow);
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(4), eq(LocalDate.of(2026, 9, 11).atStartOfDay()),
                eq("BACKFILL"), eq("backfill-node-serial:2026-09-11"), any(), eq(51L)))
                .thenReturn(dayTwoWorkflow);
        when(taskInstanceService.createInstance(
                41L, "node.dwd_orders", 4, LocalDate.of(2026, 9, 10).atStartOfDay(),
                "paimon.prod.dwd_orders.dt=2026-09-10", 51L, 61L)).thenReturn(dayOneTask);
        when(taskInstanceService.createInstance(
                42L, "node.dwd_orders", 4, LocalDate.of(2026, 9, 11).atStartOfDay(),
                "paimon.prod.dwd_orders.dt=2026-09-11", 51L, 61L)).thenReturn(dayTwoTask);

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                LocalDate.of(2026, 9, 10),
                LocalDate.of(2026, 9, 11),
                "NO_CASCADE",
                "SERIAL",
                null,
                "backfill-node-serial",
                "alice",
                "serial repair");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        verify(taskInstanceService).markSchedulableAsActionEntry(71L);
        verify(taskInstanceService).markWaitingForSnapshot(eq(72L), contains("Waiting for backfill date slot"));

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch batch = batchCaptor.getAllValues().get(1);
        assertEquals(BackfillProgressionModes.SERIAL, batch.getProgressionMode());
        assertEquals(1, batch.getMaxActiveDates());

        ArgumentCaptor<BackfillItem> itemCaptor = ArgumentCaptor.forClass(BackfillItem.class);
        verify(backfillItemRepository, times(2)).save(itemCaptor.capture());
        assertEquals(BackfillItemStatuses.INTENT_READY, itemCaptor.getAllValues().get(0).getStatus());
        assertEquals(BackfillItemStatuses.WAITING_CONCURRENCY, itemCaptor.getAllValues().get(1).getStatus());
    }

    /**
     * Verify explicit skip policy omits only complete dates with durable snapshot evidence.
     */
    @Test
    void backfillScheduleNodeSkipsOnlyFullyConfirmedDates() {
        LocalDate confirmedDate = LocalDate.of(2026, 9, 10);
        LocalDate incompleteDate = LocalDate.of(2026, 9, 11);
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode omittedUpstream = scheduleNode(
                60L,
                "node.ods_orders",
                List.of(),
                "paimon.prod.ods_orders.dt=${bizDate}");
        ScheduleNode startNode = scheduleNode(
                61L,
                "node.dwd_orders",
                List.of("node.ods_orders"),
                "paimon.prod.dwd_orders.dt=${bizDate}");
        ScheduleNode downstreamNode = scheduleNode(
                62L,
                "node.dm_orders",
                List.of("node.dwd_orders"),
                "paimon.prod.dm_orders.dt=${bizDate}");
        TaskInstance confirmedStart = confirmedTask(
                31L, 61L, confirmedDate, "paimon.prod.dwd_orders.dt=2026-09-10", "100", "101");
        TaskInstance confirmedDownstream = confirmedTask(
                32L, 62L, confirmedDate, "paimon.prod.dm_orders.dt=2026-09-10", "200", "201");
        TaskInstance incompleteStart = confirmedTask(
                33L, 61L, incompleteDate, "paimon.prod.dwd_orders.dt=2026-09-11", "101", "102");
        when(schedulingActionRepository.findByActionKey("backfill-skip-confirmed")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(omittedUpstream, startNode, downstreamNode));
        when(taskInstanceRepository.findByFlowPlanVersionIdAndBizDateAndStateOrderByUpdatedAtDesc(
                51L, confirmedDate.atStartOfDay(), SchedulingStates.SNAPSHOT_CONFIRMED))
                .thenReturn(List.of(confirmedStart, confirmedDownstream));
        when(taskInstanceRepository.findByFlowPlanVersionIdAndBizDateAndStateOrderByUpdatedAtDesc(
                51L, incompleteDate.atStartOfDay(), SchedulingStates.SNAPSHOT_CONFIRMED))
                .thenReturn(List.of(incompleteStart));
        stubBackfillBatchSave(83L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(4), eq(incompleteDate.atStartOfDay()),
                eq("BACKFILL"), eq("backfill-skip-confirmed:2026-09-11"), any(), eq(51L)))
                .thenReturn(workflow(45L, SchedulingStates.CREATED));
        when(taskInstanceService.createInstance(
                45L, "node.dwd_orders", 4, incompleteDate.atStartOfDay(),
                "paimon.prod.dwd_orders.dt=2026-09-11", 51L, 61L)).thenReturn(task(81L));
        when(taskInstanceService.createInstance(
                45L, "node.dm_orders", 4, incompleteDate.atStartOfDay(),
                "paimon.prod.dm_orders.dt=2026-09-11", 51L, 62L)).thenReturn(task(82L));

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                confirmedDate,
                incompleteDate,
                "DIRECT_DOWNSTREAM",
                "SERIAL",
                null,
                BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES,
                "backfill-skip-confirmed",
                "alice",
                "skip completed dates");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(45L), result.workflowInstanceIds());
        assertEquals(81L, result.taskInstanceId());
        assertTrue(result.message().contains("skippedDates=1"));
        verify(taskInstanceService).markSchedulableAsActionEntry(81L);
        verify(taskInstanceService).markWaitingForSnapshot(eq(82L), contains("node.dwd_orders"));
        verify(workflowInstanceService, never()).createInstance(
                eq("flow.orders"), eq(4), eq(confirmedDate.atStartOfDay()),
                any(), any(), any(), eq(51L));

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch batch = batchCaptor.getAllValues().get(1);
        assertEquals(BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES, batch.getSkipPolicy());
        assertEquals(1, batch.getSkippedDateCount());
        assertTrue(batch.getSkipEvidenceJson().containsKey("2026-09-10"));
        assertEquals(1, batch.getProducedWorkflowCount());
        assertEquals(2, batch.getTotalItemCount());
    }

    /**
     * Verify an entirely confirmed date range completes without emitting duplicate intents.
     */
    @Test
    void backfillScheduleNodeCompletesWhenEveryDateIsSkipped() {
        LocalDate confirmedDate = LocalDate.of(2026, 9, 10);
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode startNode = scheduleNode(
                61L,
                "node.dwd_orders",
                List.of(),
                "paimon.prod.dwd_orders.dt=${bizDate}");
        TaskInstance confirmedStart = confirmedTask(
                31L, 61L, confirmedDate, "paimon.prod.dwd_orders.dt=2026-09-10", "100", "101");
        when(schedulingActionRepository.findByActionKey("backfill-all-skipped")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(startNode));
        when(taskInstanceRepository.findByFlowPlanVersionIdAndBizDateAndStateOrderByUpdatedAtDesc(
                51L, confirmedDate.atStartOfDay(), SchedulingStates.SNAPSHOT_CONFIRMED))
                .thenReturn(List.of(confirmedStart));
        stubBackfillBatchSave(84L);

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                confirmedDate,
                confirmedDate,
                "NO_CASCADE",
                "PARALLEL",
                null,
                BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES,
                "backfill-all-skipped",
                "alice",
                "avoid duplicate intents");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertTrue(result.workflowInstanceIds().isEmpty());
        assertNull(result.taskInstanceId());
        assertTrue(result.message().contains("skippedDates=1"));
        verifyNoInteractions(workflowInstanceService, taskInstanceService, backfillItemRepository);

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch batch = batchCaptor.getAllValues().get(1);
        assertEquals(BackfillBatchStatuses.COMPLETED, batch.getStatus());
        assertEquals(1, batch.getSkippedDateCount());
        assertEquals(0, batch.getProducedWorkflowCount());
        assertEquals(0, batch.getTotalItemCount());
        assertTrue(batch.getSkipEvidenceJson().containsKey("2026-09-10"));
    }

    /**
     * Verify unsupported skip policies are rejected before graph expansion.
     */
    @Test
    void backfillScheduleNodeRejectsUnsupportedSkipPolicy() {
        when(schedulingActionRepository.findByActionKey("backfill-invalid-skip")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                LocalDate.of(2026, 9, 10),
                LocalDate.of(2026, 9, 11),
                "NO_CASCADE",
                "PARALLEL",
                null,
                "SKIP_ANY_EXISTING_ASSET",
                "backfill-invalid-skip",
                "alice",
                "invalid policy");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("Unsupported backfill skipPolicy"));
        verify(flowPlanVersionRepository, never()).findById(anyLong());
    }

    /**
     * Verify transitive node backfill includes all downstream nodes reachable from the start node.
     */
    @Test
    void backfillScheduleNodeCreatesTransitiveDownstreamTaskIntents() {
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode startNode = scheduleNode(61L, "node.dwd_orders", List.of(), "paimon.prod.dwd_orders");
        ScheduleNode directDownstream = scheduleNode(
                62L,
                "node.dm_orders",
                List.of("node.dwd_orders"),
                "paimon.prod.dm_orders");
        ScheduleNode transitiveDownstream = scheduleNode(
                63L,
                "node.ads_orders",
                List.of("node.dm_orders"),
                "paimon.prod.ads_orders");
        WorkflowInstance workflow = workflow(43L, SchedulingStates.CREATED);
        TaskInstance taskOne = task(75L);
        TaskInstance taskTwo = task(76L);
        TaskInstance taskThree = task(77L);
        when(schedulingActionRepository.findByActionKey("backfill-node-transitive")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        stubBackfillBatchSave(82L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(startNode, directDownstream, transitiveDownstream));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("BACKFILL"),
                eq("backfill-node-transitive:2026-09-10"),
                any(),
                eq(51L)))
                .thenReturn(workflow);
        when(taskInstanceService.createInstance(
                eq(43L),
                eq("node.dwd_orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("paimon.prod.dwd_orders"),
                eq(51L),
                eq(61L)))
                .thenReturn(taskOne);
        when(taskInstanceService.createInstance(
                eq(43L),
                eq("node.dm_orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("paimon.prod.dm_orders"),
                eq(51L),
                eq(62L)))
                .thenReturn(taskTwo);
        when(taskInstanceService.createInstance(
                eq(43L),
                eq("node.ads_orders"),
                eq(4),
                eq(LocalDate.of(2026, 9, 10).atStartOfDay()),
                eq("paimon.prod.ads_orders"),
                eq(51L),
                eq(63L)))
                .thenReturn(taskThree);

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                LocalDate.of(2026, 9, 10),
                LocalDate.of(2026, 9, 10),
                "TRANSITIVE_DOWNSTREAM",
                "PARALLEL",
                null,
                "backfill-node-transitive",
                "alice",
                "repair all downstream");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(43L), result.workflowInstanceIds());
        assertEquals(75L, result.taskInstanceId());
        assertTrue(result.message().contains("nodes=3"));
        assertTrue(result.message().contains("taskIntents=3"));
        verify(taskInstanceService).markSchedulableAsActionEntry(75L);
        verify(taskInstanceService, times(2)).markWaitingForSnapshot(anyLong(), any());
        assertExpandedBatch(1, 3);
        assertBackfillItemsSaved(3, 1, 2);
    }

    /**
     * Verify draft FlowPlan versions cannot create node backfill task intents.
     */
    @Test
    void backfillScheduleNodeRejectsDraftVersion() {
        when(schedulingActionRepository.findByActionKey("backfill-node-draft")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.findById(51L))
                .thenReturn(Optional.of(flowPlanVersion(51L, FlowPlanVersionStatuses.DRAFT)));

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                LocalDate.of(2026, 9, 10),
                LocalDate.of(2026, 9, 11),
                "NO_CASCADE",
                "PARALLEL",
                null,
                "backfill-node-draft",
                "alice",
                "should reject");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("published"));
        verify(scheduleNodeRepository, never()).findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(any());
    }

    /**
     * Verify limited parallel progression requires an explicit positive date limit.
     */
    @Test
    void backfillScheduleNodeRejectsMissingParallelLimit() {
        when(schedulingActionRepository.findByActionKey("backfill-node-invalid-limit"))
                .thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SchedulingActionResult result = schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                LocalDate.of(2026, 9, 10),
                LocalDate.of(2026, 9, 11),
                "NO_CASCADE",
                "PARALLEL_WITH_LIMIT",
                null,
                "backfill-node-invalid-limit",
                "alice",
                "should reject");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("positive maxActiveDates"));
        verify(flowPlanVersionRepository, never()).findById(any());
    }

    /**
     * Verify recovery creates a replacement batch from the earliest snapshot failure.
     */
    @Test
    void recoverBackfillBatchCreatesReplacementFromEarliestFailedDate() {
        LocalDate firstDate = LocalDate.of(2026, 9, 10);
        LocalDate failedDate = LocalDate.of(2026, 9, 11);
        LocalDate endDate = LocalDate.of(2026, 9, 12);
        BackfillBatch sourceBatch = backfillBatch(81L, BackfillBatchStatuses.FAILED);
        sourceBatch.setBizDateStart(firstDate);
        sourceBatch.setBizDateEnd(endDate);
        sourceBatch.setProgressionMode(BackfillProgressionModes.SERIAL);
        sourceBatch.setMaxActiveDates(1);
        sourceBatch.setRecoveryAttempt(0);
        BackfillItem confirmedItem = backfillItem(91L, 71L, BackfillItemStatuses.SNAPSHOT_CONFIRMED);
        confirmedItem.setBizDate(firstDate);
        BackfillItem failedItem = backfillItem(92L, 72L, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED);
        failedItem.setBizDate(failedDate);
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode omittedUpstream = scheduleNode(
                60L,
                "node.ods_orders",
                List.of(),
                "paimon.prod.ods_orders.dt=${bizDate}");
        ScheduleNode startNode = scheduleNode(
                61L,
                "node.dwd_orders",
                List.of("node.ods_orders"),
                "paimon.prod.dwd_orders.dt=${bizDate}");
        ScheduleNode downstreamNode = scheduleNode(
                62L,
                "node.dm_orders",
                List.of("node.dwd_orders"),
                "paimon.prod.dm_orders.dt=${bizDate}");
        when(schedulingActionRepository.findByActionKey("recover-backfill-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(sourceBatch));
        when(backfillBatchRepository.findBySourceBackfillBatchId(81L)).thenReturn(Optional.empty());
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(confirmedItem, failedItem));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(omittedUpstream, startNode, downstreamNode));
        stubBackfillBatchSave(82L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(4), eq(failedDate.atStartOfDay()),
                eq("BACKFILL_RECOVERY"), eq("recover-backfill-1:2026-09-11"), any(), eq(51L)))
                .thenReturn(workflow(43L, SchedulingStates.CREATED));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(4), eq(endDate.atStartOfDay()),
                eq("BACKFILL_RECOVERY"), eq("recover-backfill-1:2026-09-12"), any(), eq(51L)))
                .thenReturn(workflow(44L, SchedulingStates.CREATED));
        when(taskInstanceService.createInstance(
                43L, "node.dwd_orders", 4, failedDate.atStartOfDay(),
                "paimon.prod.dwd_orders.dt=2026-09-11", 51L, 61L)).thenReturn(task(75L));
        when(taskInstanceService.createInstance(
                43L, "node.dm_orders", 4, failedDate.atStartOfDay(),
                "paimon.prod.dm_orders.dt=2026-09-11", 51L, 62L)).thenReturn(task(76L));
        when(taskInstanceService.createInstance(
                44L, "node.dwd_orders", 4, endDate.atStartOfDay(),
                "paimon.prod.dwd_orders.dt=2026-09-12", 51L, 61L)).thenReturn(task(77L));
        when(taskInstanceService.createInstance(
                44L, "node.dm_orders", 4, endDate.atStartOfDay(),
                "paimon.prod.dm_orders.dt=2026-09-12", 51L, 62L)).thenReturn(task(78L));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                "recover-backfill-1",
                "alice",
                "retry failed dates");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(SchedulingActionTypes.RECOVER_BACKFILL, result.actionType());
        assertEquals(List.of(43L, 44L), result.workflowInstanceIds());
        assertEquals(75L, result.taskInstanceId());
        assertTrue(result.message().contains("sourceBatch=81"));
        assertTrue(result.message().contains("replacementBatch=82"));
        assertEquals(BackfillBatchStatuses.FAILED, sourceBatch.getStatus());
        verify(taskInstanceService).markSchedulableAsActionEntry(75L);
        verify(taskInstanceService).markWaitingForSnapshot(eq(77L), contains("Waiting for backfill date slot"));
        verify(taskInstanceService).markWaitingForSnapshot(eq(76L), contains("node.dwd_orders"));
        verify(taskInstanceService).markWaitingForSnapshot(eq(78L), contains("node.dwd_orders"));
        verify(snapshotConfirmationService, never()).checkTaskSnapshotProgress(anyLong(), any());
        verify(taskInstanceService, never()).createInstance(
                anyLong(), eq("node.ods_orders"), eq(4), any(), any(), eq(51L), eq(60L));

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch replacement = batchCaptor.getAllValues().get(1);
        assertEquals(82L, replacement.getId());
        assertEquals(81L, replacement.getSourceBackfillBatchId());
        assertEquals(1, replacement.getRecoveryAttempt());
        assertEquals(BackfillRecoveryStrategies.FULL_SCOPE, replacement.getRecoveryStrategy());
        assertEquals(failedDate, replacement.getBizDateStart());
        assertEquals(endDate, replacement.getBizDateEnd());
        assertEquals(BackfillBatchStatuses.EXPANDED, replacement.getStatus());
        assertEquals(2, replacement.getProducedWorkflowCount());
        assertEquals(4, replacement.getTotalItemCount());

        ArgumentCaptor<BackfillItem> itemCaptor = ArgumentCaptor.forClass(BackfillItem.class);
        verify(backfillItemRepository, times(4)).save(itemCaptor.capture());
        assertTrue(itemCaptor.getAllValues().stream().noneMatch(item -> firstDate.equals(item.getBizDate())));
        assertEquals(1, itemCaptor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.INTENT_READY.equals(item.getStatus()))
                .count());
        assertEquals(1, itemCaptor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.WAITING_CONCURRENCY.equals(item.getStatus()))
                .count());
        assertEquals(2, itemCaptor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.WAITING_DEPENDENCY.equals(item.getStatus()))
                .count());
    }

    /**
     * Verify failed-node recovery starts at one failed node and regenerates only its closed descendants.
     */
    @Test
    void recoverBackfillBatchRegeneratesFailedNodeCascade() {
        LocalDate failedDate = LocalDate.of(2026, 9, 11);
        BackfillBatch sourceBatch = backfillBatch(81L, BackfillBatchStatuses.FAILED);
        sourceBatch.setSelectedNodeCodes(List.of("node.ods_orders", "node.dwd_orders", "node.dm_orders"));
        sourceBatch.setEntryNodeCodes(List.of("node.ods_orders"));
        sourceBatch.setStartScheduleNodeId(61L);
        sourceBatch.setStartNodeCode("node.ods_orders");
        sourceBatch.setCascadePolicy(BackfillCascadePolicies.TRANSITIVE_DOWNSTREAM);
        sourceBatch.setBizDateStart(failedDate);
        sourceBatch.setBizDateEnd(failedDate);
        BackfillItem failedItem = backfillItem(92L, 72L, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED);
        failedItem.setBizDate(failedDate);
        failedItem.setNodeCode("node.dwd_orders");
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode rootNode = scheduleNode(
                61L, "node.ods_orders", List.of(), "paimon.prod.ods_orders.dt=${bizDate}");
        ScheduleNode failedNode = scheduleNode(
                62L,
                "node.dwd_orders",
                List.of("node.ods_orders"),
                "paimon.prod.dwd_orders.dt=${bizDate}");
        ScheduleNode downstreamNode = scheduleNode(
                63L,
                "node.dm_orders",
                List.of("node.dwd_orders"),
                "paimon.prod.dm_orders.dt=${bizDate}");
        when(schedulingActionRepository.findByActionKey("recover-failed-node"))
                .thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(sourceBatch));
        when(backfillBatchRepository.findBySourceBackfillBatchId(81L)).thenReturn(Optional.empty());
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(failedItem));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(rootNode, failedNode, downstreamNode));
        stubBackfillBatchSave(82L);
        when(backfillItemRepository.save(any(BackfillItem.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(4), eq(failedDate.atStartOfDay()), eq("BACKFILL_RECOVERY"),
                eq("recover-failed-node:2026-09-11"), any(), eq(51L)))
                .thenReturn(workflow(43L, SchedulingStates.CREATED));
        when(taskInstanceService.createInstance(
                43L, "node.dwd_orders", 4, failedDate.atStartOfDay(),
                "paimon.prod.dwd_orders.dt=2026-09-11", 51L, 62L)).thenReturn(task(75L));
        when(taskInstanceService.createInstance(
                43L, "node.dm_orders", 4, failedDate.atStartOfDay(),
                "paimon.prod.dm_orders.dt=2026-09-11", 51L, 63L)).thenReturn(task(76L));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                BackfillRecoveryStrategies.FAILED_NODE_CASCADE,
                "recover-failed-node",
                "alice",
                "retry failed branch");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertTrue(result.message().contains("strategy=FAILED_NODE_CASCADE"));
        verify(taskInstanceService).markSchedulableAsActionEntry(75L);
        verify(taskInstanceService).markWaitingForSnapshot(76L, "Waiting for upstream snapshot confirmation: node.dwd_orders");
        verify(taskInstanceService, never()).createInstance(
                anyLong(), eq("node.ods_orders"), eq(4), any(), any(), eq(51L), eq(61L));

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch replacement = batchCaptor.getAllValues().get(1);
        assertEquals(BackfillRecoveryStrategies.FAILED_NODE_CASCADE, replacement.getRecoveryStrategy());
        assertEquals(BackfillScopeTypes.NODE_SUBGRAPH, replacement.getScopeType());
        assertEquals("node.dwd_orders", replacement.getStartNodeCode());
        assertEquals(List.of("node.dwd_orders"), replacement.getEntryNodeCodes());
        assertEquals(List.of("node.dwd_orders", "node.dm_orders"), replacement.getSelectedNodeCodes());
    }

    /**
     * Verify failed-node recovery refuses a downstream join whose other parent is omitted.
     */
    @Test
    void recoverBackfillBatchRejectsFailedBranchWithMissingJoinParent() {
        LocalDate failedDate = LocalDate.of(2026, 9, 11);
        BackfillBatch sourceBatch = backfillBatch(81L, BackfillBatchStatuses.FAILED);
        sourceBatch.setScopeType(BackfillScopeTypes.FULL_FLOW);
        sourceBatch.setEntryNodeCodes(List.of("node.ods_orders", "node.ods_payments"));
        sourceBatch.setSelectedNodeCodes(
                List.of("node.ods_orders", "node.ods_payments", "node.dwd_order_payments"));
        sourceBatch.setStartScheduleNodeId(null);
        sourceBatch.setStartNodeCode(null);
        sourceBatch.setCascadePolicy(null);
        sourceBatch.setBizDateStart(failedDate);
        sourceBatch.setBizDateEnd(failedDate);
        BackfillItem failedItem = backfillItem(91L, 71L, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED);
        failedItem.setBizDate(failedDate);
        failedItem.setNodeCode("node.ods_orders");
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode ordersRoot = scheduleNode(
                61L, "node.ods_orders", List.of(), "paimon.prod.ods_orders.dt=${bizDate}");
        ScheduleNode paymentsRoot = scheduleNode(
                62L, "node.ods_payments", List.of(), "paimon.prod.ods_payments.dt=${bizDate}");
        ScheduleNode joinedNode = scheduleNode(
                63L,
                "node.dwd_order_payments",
                List.of("node.ods_orders", "node.ods_payments"),
                "paimon.prod.dwd_order_payments.dt=${bizDate}");
        when(schedulingActionRepository.findByActionKey("recover-open-join"))
                .thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(sourceBatch));
        when(backfillBatchRepository.findBySourceBackfillBatchId(81L)).thenReturn(Optional.empty());
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(failedItem));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(ordersRoot, paymentsRoot, joinedNode));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                BackfillRecoveryStrategies.FAILED_NODE_CASCADE,
                "recover-open-join",
                "alice",
                "retry failed branch");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("missing direct parents node.ods_payments"));
        verify(backfillBatchRepository, never()).save(any(BackfillBatch.class));
        verify(workflowInstanceService, never()).createInstance(any(), any(), any(), any(), any(), any(), any());
    }

    /**
     * Verify multiple independent failed nodes require the deterministic full-scope fallback.
     */
    @Test
    void recoverBackfillBatchRejectsMultipleFailedNodeCascadeEntries() {
        LocalDate failedDate = LocalDate.of(2026, 9, 11);
        BackfillBatch sourceBatch = backfillBatch(81L, BackfillBatchStatuses.FAILED);
        sourceBatch.setScopeType(BackfillScopeTypes.FULL_FLOW);
        sourceBatch.setEntryNodeCodes(List.of("node.ods_orders", "node.ods_payments"));
        sourceBatch.setSelectedNodeCodes(List.of("node.ods_orders", "node.ods_payments"));
        sourceBatch.setStartScheduleNodeId(null);
        sourceBatch.setStartNodeCode(null);
        sourceBatch.setCascadePolicy(null);
        sourceBatch.setBizDateStart(failedDate);
        sourceBatch.setBizDateEnd(failedDate);
        BackfillItem ordersFailure = backfillItem(91L, 71L, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED);
        ordersFailure.setBizDate(failedDate);
        ordersFailure.setNodeCode("node.ods_orders");
        BackfillItem paymentsFailure = backfillItem(92L, 72L, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED);
        paymentsFailure.setBizDate(failedDate);
        paymentsFailure.setNodeCode("node.ods_payments");
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode ordersRoot = scheduleNode(
                61L, "node.ods_orders", List.of(), "paimon.prod.ods_orders.dt=${bizDate}");
        ScheduleNode paymentsRoot = scheduleNode(
                62L, "node.ods_payments", List.of(), "paimon.prod.ods_payments.dt=${bizDate}");
        when(schedulingActionRepository.findByActionKey("recover-multiple-failures"))
                .thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(sourceBatch));
        when(backfillBatchRepository.findBySourceBackfillBatchId(81L)).thenReturn(Optional.empty());
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(ordersFailure, paymentsFailure));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(ordersRoot, paymentsRoot));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                BackfillRecoveryStrategies.FAILED_NODE_CASCADE,
                "recover-multiple-failures",
                "alice",
                "retry failed branches");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("exactly one failed node"));
        assertTrue(result.message().contains("use FULL_SCOPE"));
        verify(backfillBatchRepository, never()).save(any(BackfillBatch.class));
    }

    /**
     * Verify recovery preserves a complete-Flow batch and all of its root entries.
     */
    @Test
    void recoverBackfillBatchPreservesCompleteFlowScope() {
        LocalDate failedDate = LocalDate.of(2026, 9, 11);
        BackfillBatch sourceBatch = backfillBatch(81L, BackfillBatchStatuses.FAILED);
        sourceBatch.setScopeType(BackfillScopeTypes.FULL_FLOW);
        sourceBatch.setEntryNodeCodes(List.of("node.ods_orders", "node.ods_payments"));
        sourceBatch.setSelectedNodeCodes(
                List.of("node.ods_orders", "node.ods_payments", "node.dwd_order_payments"));
        sourceBatch.setStartScheduleNodeId(null);
        sourceBatch.setStartNodeCode(null);
        sourceBatch.setCascadePolicy(null);
        sourceBatch.setBizDateStart(failedDate);
        sourceBatch.setBizDateEnd(failedDate);
        BackfillItem failedItem = backfillItem(91L, 71L, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED);
        failedItem.setBizDate(failedDate);
        FlowPlanVersion version = flowPlanVersion(51L, FlowPlanVersionStatuses.PUBLISHED);
        ScheduleNode ordersRoot = scheduleNode(
                61L, "node.ods_orders", List.of(), "paimon.prod.ods_orders.dt=${bizDate}");
        ScheduleNode paymentsRoot = scheduleNode(
                62L, "node.ods_payments", List.of(), "paimon.prod.ods_payments.dt=${bizDate}");
        ScheduleNode joinedNode = scheduleNode(
                63L,
                "node.dwd_order_payments",
                List.of("node.ods_orders", "node.ods_payments"),
                "paimon.prod.dwd_order_payments.dt=${bizDate}");
        when(schedulingActionRepository.findByActionKey("recover-full-flow")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(sourceBatch));
        when(backfillBatchRepository.findBySourceBackfillBatchId(81L)).thenReturn(Optional.empty());
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(failedItem));
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(ordersRoot, paymentsRoot, joinedNode));
        stubBackfillBatchSave(82L);
        when(backfillItemRepository.save(any(BackfillItem.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(workflowInstanceService.createInstance(
                eq("flow.orders"), eq(4), eq(failedDate.atStartOfDay()), eq("BACKFILL_RECOVERY"),
                eq("recover-full-flow:2026-09-11"), any(), eq(51L)))
                .thenReturn(workflow(43L, SchedulingStates.CREATED));
        when(taskInstanceService.createInstance(
                anyLong(), any(), eq(4), eq(failedDate.atStartOfDay()), any(), eq(51L), anyLong()))
                .thenReturn(task(75L), task(76L), task(77L));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                "recover-full-flow",
                "alice",
                "retry complete flow");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(43L), result.workflowInstanceIds());
        verify(taskInstanceService).markSchedulableAsActionEntry(75L);
        verify(taskInstanceService).markSchedulableAsActionEntry(76L);
        verify(taskInstanceService).markWaitingForSnapshot(eq(77L), contains("node.ods_orders,node.ods_payments"));

        ArgumentCaptor<BackfillBatch> batchCaptor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(batchCaptor.capture());
        BackfillBatch replacement = batchCaptor.getAllValues().get(1);
        assertEquals(BackfillScopeTypes.FULL_FLOW, replacement.getScopeType());
        assertEquals(List.of("node.ods_orders", "node.ods_payments"), replacement.getEntryNodeCodes());
        assertEquals(
                List.of("node.ods_orders", "node.ods_payments", "node.dwd_order_payments"),
                replacement.getSelectedNodeCodes());
        assertNull(replacement.getStartNodeCode());
        assertNull(replacement.getCascadePolicy());
        assertEquals(81L, replacement.getSourceBackfillBatchId());
    }

    /**
     * Verify an unknown recovery strategy is rejected before a source batch is changed.
     */
    @Test
    void recoverBackfillBatchRejectsUnsupportedRecoveryStrategy() {
        when(schedulingActionRepository.findByActionKey("recover-invalid-strategy"))
                .thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                "RETRY_ANY_FAILURE",
                "recover-invalid-strategy",
                "alice",
                "invalid strategy");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("Unsupported backfill recoveryStrategy"));
        verify(backfillBatchRepository, never()).findByIdForUpdate(anyLong());
        verify(backfillBatchRepository, never()).save(any(BackfillBatch.class));
    }

    /**
     * Verify recovery rejects batches that have not failed snapshot confirmation.
     */
    @Test
    void recoverBackfillBatchRejectsNonFailedBatch() {
        BackfillBatch sourceBatch = backfillBatch(81L, BackfillBatchStatuses.EXPANDED);
        when(schedulingActionRepository.findByActionKey("recover-active-backfill")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(sourceBatch));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                "recover-active-backfill",
                "alice",
                "invalid recovery");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains(BackfillBatchStatuses.EXPANDED));
        verify(backfillBatchRepository, never()).findBySourceBackfillBatchId(anyLong());
        verify(flowPlanVersionRepository, never()).findById(anyLong());
    }

    /**
     * Verify recovery requires explicit snapshot-not-advanced evidence.
     */
    @Test
    void recoverBackfillBatchRejectsFailedBatchWithoutSnapshotFailureItem() {
        BackfillBatch sourceBatch = backfillBatch(81L, BackfillBatchStatuses.FAILED);
        BackfillItem confirmedItem = backfillItem(91L, 71L, BackfillItemStatuses.SNAPSHOT_CONFIRMED);
        confirmedItem.setBizDate(LocalDate.of(2026, 9, 10));
        when(schedulingActionRepository.findByActionKey("recover-without-failure")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(sourceBatch));
        when(backfillBatchRepository.findBySourceBackfillBatchId(81L)).thenReturn(Optional.empty());
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(confirmedItem));

        SchedulingActionResult result = schedulingActionService.recoverBackfillBatch(
                81L,
                "recover-without-failure",
                "alice",
                "invalid recovery");

        assertEquals(SchedulingActionStatuses.REJECTED, result.status());
        assertTrue(result.message().contains("no SNAPSHOT_NOT_ADVANCED item"));
        verify(flowPlanVersionRepository, never()).findById(anyLong());
    }

    /**
     * Verify pausing a backfill batch disables pending intent delivery at batch level.
     */
    @Test
    void pauseBackfillBatchMovesExpandedBatchToPaused() {
        BackfillBatch batch = backfillBatch(81L, BackfillBatchStatuses.EXPANDED);
        when(schedulingActionRepository.findByActionKey("pause-backfill-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(batch));

        SchedulingActionResult result = schedulingActionService.pauseBackfillBatch(
                81L,
                "pause-backfill-1",
                "alice",
                "hold delivery");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(SchedulingActionTypes.PAUSE_BACKFILL, result.actionType());
        assertEquals(BackfillBatchStatuses.PAUSED, batch.getStatus());
        verify(backfillBatchRepository).save(batch);
    }

    /**
     * Verify resuming a paused backfill batch makes its pending intents deliverable again.
     */
    @Test
    void resumeBackfillBatchMovesPausedBatchToExpanded() {
        BackfillBatch batch = backfillBatch(81L, BackfillBatchStatuses.PAUSED);
        when(schedulingActionRepository.findByActionKey("resume-backfill-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(batch));

        SchedulingActionResult result = schedulingActionService.resumeBackfillBatch(
                81L,
                "resume-backfill-1",
                "alice",
                "continue delivery");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(SchedulingActionTypes.RESUME_BACKFILL, result.actionType());
        assertEquals(BackfillBatchStatuses.EXPANDED, batch.getStatus());
        verify(backfillBatchRepository).save(batch);
        verify(backfillProgressionService).refreshBatch(81L);
    }

    /**
     * Verify batch cancellation cancels pending intents but retains already delivered intents.
     */
    @Test
    void cancelBackfillBatchOnlyCancelsPendingIntents() {
        BackfillBatch batch = backfillBatch(81L, BackfillBatchStatuses.PAUSED);
        BackfillItem pendingItem = backfillItem(91L, 71L, BackfillItemStatuses.INTENT_READY);
        BackfillItem deliveredItem = backfillItem(92L, 72L, BackfillItemStatuses.INTENT_READY);
        TaskInstance pendingTask = task(71L);
        TaskInstance deliveredTask = task(72L);
        deliveredTask.setState(SchedulingStates.SCHEDULED);
        when(schedulingActionRepository.findByActionKey("cancel-backfill-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(batch));
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(pendingItem, deliveredItem));
        when(taskInstanceRepository.findById(71L)).thenReturn(Optional.of(pendingTask));
        when(taskInstanceRepository.findById(72L)).thenReturn(Optional.of(deliveredTask));
        when(backfillItemRepository.save(any(BackfillItem.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SchedulingActionResult result = schedulingActionService.cancelBackfillBatch(
                81L,
                "cancel-backfill-1",
                "alice",
                "operator cancel");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(SchedulingActionTypes.CANCEL_BACKFILL, result.actionType());
        assertTrue(result.message().contains("pendingIntents=1"));
        assertTrue(result.message().contains("deliveredIntentsRetained=1"));
        assertEquals(BackfillBatchStatuses.CANCELLED, batch.getStatus());
        assertEquals(BackfillItemStatuses.CANCELLED, pendingItem.getStatus());
        assertEquals(BackfillItemStatuses.INTENT_DELIVERED, deliveredItem.getStatus());
        verify(taskInstanceService).cancel(71L, "Backfill batch cancelled: operator cancel");
        verify(taskInstanceService, never()).cancel(eq(72L), any());
    }

    /**
     * Verify skip only changes scheduler-side task state.
     */
    @Test
    void skipTaskInstanceMarksTaskSkipped() {
        when(schedulingActionRepository.findByActionKey("skip-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task(22L)));

        SchedulingActionResult result = schedulingActionService.skipTaskInstance(
                22L,
                "skip-1",
                "alice",
                "manual skip");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(22L, result.taskInstanceId());
        verify(taskInstanceService).markSkipped(22L, "manual skip");
    }

    /**
     * Verify workflow cancel only changes scheduler-side workflow state.
     */
    @Test
    void cancelWorkflowInstanceMarksWorkflowCancelled() {
        when(schedulingActionRepository.findByActionKey("cancel-workflow-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        TaskInstance readyTask = task(22L);
        when(workflowInstanceRepository.findByIdForUpdate(11L))
                .thenReturn(Optional.of(workflow(11L, SchedulingStates.SCHEDULED)));
        when(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(11L))
                .thenReturn(List.of(readyTask));

        SchedulingActionResult result = schedulingActionService.cancelWorkflowInstance(
                11L,
                "cancel-workflow-1",
                "alice",
                "operator cancel");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(List.of(11L), result.workflowInstanceIds());
        verify(taskInstanceService).cancel(22L, "Workflow instance cancelled: operator cancel");
        verify(workflowInstanceService).cancel(11L);
    }

    /**
     * Verify task cancel only changes scheduler-side task state.
     */
    @Test
    void cancelTaskInstanceMarksTaskCancelled() {
        when(schedulingActionRepository.findByActionKey("cancel-task-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task(22L)));

        SchedulingActionResult result = schedulingActionService.cancelTaskInstance(
                22L,
                "cancel-task-1",
                "alice",
                "operator cancel");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertEquals(22L, result.taskInstanceId());
        verify(taskInstanceService).cancel(22L, "operator cancel");
    }

    /**
     * Verify duplicate action keys return a deduped result without side effects.
     */
    @Test
    void duplicateActionKeyReturnsDedupedResult() {
        SchedulingAction existing = SchedulingAction.builder()
                .actionKey("rerun-1")
                .actionType(SchedulingActionTypes.RERUN_WORKFLOW)
                .status(SchedulingActionStatuses.APPLIED)
                .producedWorkflowInstanceId(21L)
                .build();
        when(schedulingActionRepository.findByActionKey("rerun-1")).thenReturn(Optional.of(existing));

        SchedulingActionResult result = schedulingActionService.rerunWorkflowInstance(
                11L,
                "rerun-1",
                "alice",
                "operator retry");

        assertEquals(SchedulingActionStatuses.DEDUPED, result.status());
        assertEquals(List.of(21L), result.workflowInstanceIds());
        verify(workflowInstanceService, never()).createInstance(any(), any(), any(), any(), any(), any());
    }

    /**
     * Verify snapshot recheck delegates to confirmation using no timeout expiry.
     */
    @Test
    void recheckTaskSnapshotDelegatesToSnapshotConfirmation() {
        TaskInstance task = task(22L);
        task.setTargetAssetKey("paimon.prod.dwd_orders");
        when(schedulingActionRepository.findByActionKey("recheck-1")).thenReturn(Optional.empty());
        when(schedulingActionRepository.save(any(SchedulingAction.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));
        when(snapshotConfirmationService.checkTaskSnapshotProgress(22L, null))
                .thenReturn(new SnapshotConfirmationResult(
                        22L,
                        "paimon.prod.dwd_orders",
                        "100",
                        "101",
                        SchedulingStates.SNAPSHOT_CONFIRMED,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null));

        SchedulingActionResult result = schedulingActionService.recheckTaskSnapshot(
                22L,
                "recheck-1",
                "alice",
                "confirm now");

        assertEquals(SchedulingActionStatuses.APPLIED, result.status());
        assertTrue(result.message().contains(SchedulingStates.SNAPSHOT_CONFIRMED));
        verify(snapshotConfirmationService).checkTaskSnapshotProgress(22L, null);
    }

    /**
     * Build a workflow instance fixture.
     */
    private WorkflowInstance workflow(Long id, String state) {
        return WorkflowInstance.builder()
                .id(id)
                .workflowCode("flow.orders")
                .workflowVersion(2)
                .bizDate(BIZ_DATE)
                .state(state)
                .build();
    }

    /**
     * Build a FlowPlanVersion fixture.
     */
    private FlowPlanVersion flowPlanVersion(Long id, String status) {
        return FlowPlanVersion.builder()
                .id(id)
                .flowPlanId(41L)
                .flowCode("flow.orders")
                .version(4)
                .status(status)
                .build();
    }

    /**
     * Build a ScheduleNode fixture.
     */
    private ScheduleNode scheduleNode(Long id) {
        return scheduleNode(id, "node.dwd_orders", List.of(), "paimon.prod.dwd_orders");
    }

    /**
     * Build a ScheduleNode fixture with dependency metadata.
     */
    private ScheduleNode scheduleNode(Long id, String nodeCode, List<String> dependsOnNodes, String outputAssetKey) {
        return ScheduleNode.builder()
                .id(id)
                .flowPlanVersionId(51L)
                .nodeCode(nodeCode)
                .nodeName(nodeCode)
                .nodeType("ASSET_OUTPUT")
                .dependsOnNodes(dependsOnNodes)
                .outputAssetKey(outputAssetKey)
                .build();
    }

    /**
     * Stub backfill batch persistence with a generated id.
     */
    private void stubBackfillBatchSave(Long batchId) {
        when(backfillBatchRepository.save(any(BackfillBatch.class))).thenAnswer(invocation -> {
            BackfillBatch batch = invocation.getArgument(0);
            if (batch.getId() == null) {
                batch.setId(batchId);
            }
            return batch;
        });
    }

    /**
     * Verify the final batch update records expansion counts.
     */
    private void assertExpandedBatch(int workflowCount, int itemCount) {
        ArgumentCaptor<BackfillBatch> captor = ArgumentCaptor.forClass(BackfillBatch.class);
        verify(backfillBatchRepository, times(2)).save(captor.capture());
        BackfillBatch batch = captor.getAllValues().get(captor.getAllValues().size() - 1);
        assertEquals(BackfillBatchStatuses.EXPANDED, batch.getStatus());
        assertEquals(workflowCount, batch.getProducedWorkflowCount());
        assertEquals(itemCount, batch.getTotalItemCount());
    }

    /**
     * Verify backfill items are persisted as ready scheduling intents.
     */
    private void assertBackfillItemsSaved(int itemCount, long readyCount, long waitingCount) {
        ArgumentCaptor<BackfillItem> captor = ArgumentCaptor.forClass(BackfillItem.class);
        verify(backfillItemRepository, times(itemCount)).save(captor.capture());
        assertEquals(itemCount, captor.getAllValues().size());
        assertEquals(readyCount, captor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.INTENT_READY.equals(item.getStatus()))
                .count());
        assertEquals(waitingCount, captor.getAllValues().stream()
                .filter(item -> BackfillItemStatuses.WAITING_DEPENDENCY.equals(item.getStatus()))
                .count());
    }

    /**
     * Build a backfill batch fixture for delivery-control actions.
     */
    private BackfillBatch backfillBatch(Long id, String status) {
        return BackfillBatch.builder()
                .id(id)
                .batchKey("backfill-node-1")
                .actionKey("backfill-node-1")
                .flowPlanVersionId(51L)
                .workflowCode("flow.orders")
                .workflowVersion(4)
                .scopeType(BackfillScopeTypes.NODE_SUBGRAPH)
                .entryNodeCodes(List.of("node.dwd_orders"))
                .selectedNodeCodes(List.of("node.dwd_orders", "node.dm_orders"))
                .startScheduleNodeId(61L)
                .startNodeCode("node.dwd_orders")
                .bizDateStart(LocalDate.of(2026, 9, 10))
                .bizDateEnd(LocalDate.of(2026, 9, 11))
                .cascadePolicy("DIRECT_DOWNSTREAM")
                .progressionMode(BackfillProgressionModes.PARALLEL)
                .skipPolicy(BackfillSkipPolicies.NONE)
                .recoveryAttempt(0)
                .status(status)
                .build();
    }

    /**
     * Build a backfill item fixture for batch cancellation.
     */
    private BackfillItem backfillItem(Long id, Long taskInstanceId, String status) {
        return BackfillItem.builder()
                .id(id)
                .backfillBatchId(81L)
                .taskInstanceId(taskInstanceId)
                .status(status)
                .build();
    }

    /**
     * Build a task instance fixture.
     */
    private TaskInstance task(Long id) {
        return TaskInstance.builder()
                .id(id)
                .workflowInstanceId(11L)
                .taskCode("node.dwd_orders")
                .taskVersion(1)
                .bizDate(BIZ_DATE)
                .state(SchedulingStates.READY_TO_SCHEDULE)
                .build();
    }

    /**
     * Build historical task evidence for an explicit whole-date skip decision.
     *
     * @param id task id
     * @param scheduleNodeId immutable node id
     * @param bizDate business date represented by the task
     * @param targetAssetKey date-resolved target asset
     * @param baselineSnapshotId snapshot captured before delivery
     * @param observedSnapshotId snapshot that confirmed progress
     * @return snapshot-confirmed task fixture
     */
    private TaskInstance confirmedTask(
            Long id,
            Long scheduleNodeId,
            LocalDate bizDate,
            String targetAssetKey,
            String baselineSnapshotId,
            String observedSnapshotId) {

        return TaskInstance.builder()
                .id(id)
                .workflowInstanceId(11L)
                .taskCode("node.evidence")
                .taskVersion(4)
                .flowPlanVersionId(51L)
                .scheduleNodeId(scheduleNodeId)
                .bizDate(bizDate.atStartOfDay())
                .state(SchedulingStates.SNAPSHOT_CONFIRMED)
                .targetAssetKey(targetAssetKey)
                .baselineSnapshotId(baselineSnapshotId)
                .observedSnapshotId(observedSnapshotId)
                .build();
    }
}
