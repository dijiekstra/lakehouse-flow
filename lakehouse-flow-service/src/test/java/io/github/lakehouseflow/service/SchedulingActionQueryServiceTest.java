package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.common.BackfillScopeTypes;
import io.github.lakehouseflow.common.BackfillSkipPolicies;
import io.github.lakehouseflow.common.SchedulingActionStatuses;
import io.github.lakehouseflow.common.SchedulingActionTypes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.SchedulingActionRepository;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.SchedulingAction;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.model.TaskInstance;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests every public SchedulingActionQueryService query entry point.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingActionQueryServiceTest {

    private static final LocalDate BIZ_DATE = LocalDate.of(2026, 9, 10);
    private static final LocalDateTime CREATED_AT = LocalDateTime.of(2026, 9, 13, 10, 0);

    @Mock
    private SchedulingActionRepository schedulingActionRepository;

    @Mock
    private BackfillBatchRepository backfillBatchRepository;

    @Mock
    private BackfillItemRepository backfillItemRepository;

    @Mock
    private TaskInstanceRepository taskInstanceRepository;

    @Mock
    private SchedulingIntentRepository schedulingIntentRepository;

    @Mock
    private SchedulingIntentDeliveryRepository schedulingIntentDeliveryRepository;

    @InjectMocks
    private SchedulingActionQueryService schedulingActionQueryService;

    /**
     * Verify a backfill action joins ordered batch items to target snapshot evidence.
     */
    @Test
    void findActionByKeyReturnsBackfillBatchAndOrderedSnapshotEvidence() {
        SchedulingAction action = action("backfill-node-1", SchedulingActionTypes.BACKFILL_NODE, "SCHEDULE_NODE");
        BackfillBatch batch = batch();
        BackfillItem firstItem = item(91L, 71L, "node.dwd_orders");
        BackfillItem secondItem = item(92L, 72L, "node.dm_orders");
        TaskInstance firstTask = task(71L, "node.dwd_orders", "10", "11");
        TaskInstance secondTask = task(72L, "node.dm_orders", "10", "10");

        when(schedulingActionRepository.findByActionKey("backfill-node-1"))
                .thenReturn(Optional.of(action));
        when(backfillBatchRepository.findByActionKey("backfill-node-1"))
                .thenReturn(Optional.of(batch));
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(firstItem, secondItem));
        when(taskInstanceRepository.findAllById(List.of(71L, 72L)))
                .thenReturn(List.of(secondTask, firstTask));
        SchedulingIntent intent = SchedulingIntent.builder()
                .id(101L)
                .intentKey("task-instance:71")
                .taskInstanceId(71L)
                .build();
        SchedulingIntentDelivery delivery = SchedulingIntentDelivery.builder()
                .schedulingIntentId(101L)
                .channel("DATABASE_TABLE")
                .status("PUBLISHED")
                .attemptCount(1)
                .publishedAt(CREATED_AT)
                .build();
        when(schedulingIntentRepository.findByTaskInstanceId(71L)).thenReturn(Optional.of(intent));
        when(schedulingIntentDeliveryRepository.findBySchedulingIntentId(101L))
                .thenReturn(Optional.of(delivery));

        SchedulingActionQueryService.ActionDetailView result =
                schedulingActionQueryService.findActionByKey(" backfill-node-1 ").orElseThrow();

        assertEquals(81L, result.backfillBatch().id());
        assertEquals(BackfillScopeTypes.NODE_SUBGRAPH, result.backfillBatch().scopeType());
        assertEquals(2, result.snapshotEvidence().size());
        assertEquals(71L, result.snapshotEvidence().get(0).taskInstanceId());
        assertTrue(result.snapshotEvidence().get(0).snapshotAdvanced());
        assertEquals("task-instance:71", result.snapshotEvidence().get(0).schedulingIntentKey());
        assertEquals("PUBLISHED", result.snapshotEvidence().get(0).deliveryStatus());
        assertEquals(1, result.snapshotEvidence().get(0).deliveryAttemptCount());
        assertEquals(72L, result.snapshotEvidence().get(1).taskInstanceId());
        assertFalse(result.snapshotEvidence().get(1).snapshotAdvanced());
    }

    /**
     * Verify a batch-control action follows its structured payload to the controlled batch.
     */
    @Test
    void findActionByKeyResolvesControlledBackfillBatchFromPayload() {
        SchedulingAction action = action(
                "pause-backfill-1",
                SchedulingActionTypes.PAUSE_BACKFILL,
                "BACKFILL_BATCH");
        action.setRequestPayloadJson(Map.of("backfillBatchId", "81"));

        when(schedulingActionRepository.findByActionKey("pause-backfill-1"))
                .thenReturn(Optional.of(action));
        when(backfillBatchRepository.findByActionKey("pause-backfill-1"))
                .thenReturn(Optional.empty());
        when(backfillBatchRepository.findById(81L)).thenReturn(Optional.of(batch()));
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of());

        SchedulingActionQueryService.ActionDetailView result =
                schedulingActionQueryService.findActionByKey("pause-backfill-1").orElseThrow();

        assertEquals(81L, result.backfillBatch().id());
        assertTrue(result.snapshotEvidence().isEmpty());
    }

    /**
     * Verify non-backfill workflow actions expose all affected task snapshot evidence.
     */
    @Test
    void findActionByKeyUsesProducedWorkflowForNonBackfillEvidence() {
        SchedulingAction action = action(
                "rerun-workflow-1",
                SchedulingActionTypes.RERUN_WORKFLOW,
                "WORKFLOW_INSTANCE");
        action.setProducedWorkflowInstanceId(41L);
        when(schedulingActionRepository.findByActionKey("rerun-workflow-1"))
                .thenReturn(Optional.of(action));
        when(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(41L))
                .thenReturn(List.of(task(71L, "node.dwd_orders", null, "1")));

        SchedulingActionQueryService.ActionDetailView result =
                schedulingActionQueryService.findActionByKey("rerun-workflow-1").orElseThrow();

        assertNull(result.backfillBatch());
        assertEquals(1, result.snapshotEvidence().size());
        assertTrue(result.snapshotEvidence().get(0).snapshotAdvanced());
        verify(backfillBatchRepository, never()).findByActionKey(any());
    }

    /**
     * Verify a newer unattributed observation does not appear as confirmed progress.
     */
    @Test
    void findActionByKeyKeepsUnconfirmedSnapshotProgressUnknown() {
        SchedulingAction action = action(
                "rerun-task-1",
                SchedulingActionTypes.RERUN_TASK,
                "TASK_INSTANCE");
        action.setProducedTaskInstanceId(71L);
        TaskInstance waitingTask = task(71L, "node.dwd_orders", "10", "11");
        waitingTask.setState(SchedulingStates.SCHEDULED);
        when(schedulingActionRepository.findByActionKey("rerun-task-1"))
                .thenReturn(Optional.of(action));
        when(taskInstanceRepository.findById(71L)).thenReturn(Optional.of(waitingTask));

        SchedulingActionQueryService.ActionDetailView result =
                schedulingActionQueryService.findActionByKey("rerun-task-1").orElseThrow();

        assertEquals(1, result.snapshotEvidence().size());
        assertNull(result.snapshotEvidence().get(0).snapshotAdvanced());
    }

    /**
     * Verify blank keys return no action without querying persistence.
     */
    @Test
    void findActionByKeyReturnsEmptyForBlankKey() {
        assertTrue(schedulingActionQueryService.findActionByKey(" ").isEmpty());

        verify(schedulingActionRepository, never()).findByActionKey(any());
    }

    /**
     * Verify combined definition filters are normalized and result limits are capped.
     */
    @Test
    void searchActionsNormalizesFiltersAndCapsLimit() {
        SchedulingAction action = action(
                "rerun-node-1",
                SchedulingActionTypes.RERUN_TASK,
                "SCHEDULE_NODE");
        when(schedulingActionRepository.searchByDefinitionAnchors(
                org.mockito.ArgumentMatchers.eq("flow.orders"),
                org.mockito.ArgumentMatchers.eq(51L),
                org.mockito.ArgumentMatchers.eq(61L),
                any(Pageable.class)))
                .thenReturn(List.of(action));

        List<SchedulingActionQueryService.ActionSummaryView> result =
                schedulingActionQueryService.searchActions(" flow.orders ", 51L, 61L, 500);

        ArgumentCaptor<Pageable> pageable = ArgumentCaptor.forClass(Pageable.class);
        verify(schedulingActionRepository).searchByDefinitionAnchors(
                org.mockito.ArgumentMatchers.eq("flow.orders"),
                org.mockito.ArgumentMatchers.eq(51L),
                org.mockito.ArgumentMatchers.eq(61L),
                pageable.capture());
        assertEquals(200, pageable.getValue().getPageSize());
        assertEquals("rerun-node-1", result.get(0).actionKey());
    }

    /**
     * Verify action search rejects requests with no Flow definition anchor.
     */
    @Test
    void searchActionsRejectsMissingDefinitionAnchors() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> schedulingActionQueryService.searchActions(" ", null, null, null));

        assertTrue(exception.getMessage().contains("At least one"));
        verify(schedulingActionRepository, never()).searchByDefinitionAnchors(any(), any(), any(), any());
    }

    /**
     * Verify action search validates identifiers and explicit result limits.
     */
    @Test
    void searchActionsRejectsInvalidIdentifiersAndLimits() {
        assertThrows(
                IllegalArgumentException.class,
                () -> schedulingActionQueryService.searchActions(null, 0L, null, 10));
        assertThrows(
                IllegalArgumentException.class,
                () -> schedulingActionQueryService.searchActions("flow.orders", null, null, 0));

        verify(schedulingActionRepository, never()).searchByDefinitionAnchors(any(), any(), any(), any());
    }

    /**
     * Build a scheduling action fixture.
     *
     * @param actionKey action key
     * @param actionType action type
     * @param scopeType scope type
     * @return persisted action fixture
     */
    private SchedulingAction action(String actionKey, String actionType, String scopeType) {
        return SchedulingAction.builder()
                .id(31L)
                .actionKey(actionKey)
                .actionType(actionType)
                .scopeType(scopeType)
                .workflowCode("flow.orders")
                .workflowVersion(4)
                .flowPlanVersionId(51L)
                .scheduleNodeId(61L)
                .bizDateStart(BIZ_DATE)
                .bizDateEnd(BIZ_DATE.plusDays(1))
                .requestedBy("alice")
                .reason("repair partitions")
                .status(SchedulingActionStatuses.APPLIED)
                .resultMessage("Scheduling decision emitted")
                .requestPayloadJson(Map.of("nodeCode", "node.dwd_orders"))
                .createdAt(CREATED_AT)
                .updatedAt(CREATED_AT.plusMinutes(1))
                .build();
    }

    /**
     * Build a unified backfill batch fixture.
     *
     * @return persisted batch fixture
     */
    private BackfillBatch batch() {
        return BackfillBatch.builder()
                .id(81L)
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
                .bizDateStart(BIZ_DATE)
                .bizDateEnd(BIZ_DATE.plusDays(1))
                .cascadePolicy("ALL_DOWNSTREAM")
                .progressionMode(BackfillProgressionModes.SERIAL)
                .skipPolicy(BackfillSkipPolicies.NONE)
                .skippedDateCount(0)
                .skipEvidenceJson(Map.of())
                .recoveryAttempt(0)
                .status(BackfillBatchStatuses.EXPANDED)
                .producedWorkflowCount(2)
                .totalItemCount(4)
                .createdAt(CREATED_AT)
                .updatedAt(CREATED_AT)
                .build();
    }

    /**
     * Build a backfill item fixture.
     *
     * @param id item id
     * @param taskInstanceId linked task id
     * @param nodeCode node code
     * @return persisted item fixture
     */
    private BackfillItem item(Long id, Long taskInstanceId, String nodeCode) {
        return BackfillItem.builder()
                .id(id)
                .backfillBatchId(81L)
                .bizDate(BIZ_DATE)
                .flowPlanVersionId(51L)
                .scheduleNodeId(id - 30)
                .nodeCode(nodeCode)
                .targetAssetKey("paimon.prod." + nodeCode)
                .workflowInstanceId(41L)
                .taskInstanceId(taskInstanceId)
                .status(BackfillItemStatuses.INTENT_DELIVERED)
                .createdAt(CREATED_AT)
                .updatedAt(CREATED_AT)
                .build();
    }

    /**
     * Build task snapshot evidence for an action.
     *
     * @param id task id
     * @param taskCode task code
     * @param baselineSnapshotId delivery-time baseline snapshot
     * @param observedSnapshotId latest observed snapshot
     * @return persisted task fixture
     */
    private TaskInstance task(
            Long id,
            String taskCode,
            String baselineSnapshotId,
            String observedSnapshotId) {
        return TaskInstance.builder()
                .id(id)
                .instanceKey("41:" + taskCode)
                .workflowInstanceId(41L)
                .taskCode(taskCode)
                .taskVersion(4)
                .flowPlanVersionId(51L)
                .scheduleNodeId(id - 10)
                .bizDate(BIZ_DATE.atStartOfDay())
                .state(baselineSnapshotId != null && baselineSnapshotId.equals(observedSnapshotId)
                        ? SchedulingStates.SNAPSHOT_NOT_ADVANCED
                        : SchedulingStates.SNAPSHOT_CONFIRMED)
                .targetAssetKey("paimon.prod." + taskCode)
                .baselineSnapshotId(baselineSnapshotId)
                .observedSnapshotId(observedSnapshotId)
                .scheduledAt(CREATED_AT)
                .lastSnapshotCheckAt(CREATED_AT.plusMinutes(5))
                .createdAt(CREATED_AT)
                .updatedAt(CREATED_AT.plusMinutes(5))
                .build();
    }
}
