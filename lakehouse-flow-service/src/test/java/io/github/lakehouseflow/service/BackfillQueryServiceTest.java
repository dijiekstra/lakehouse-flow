package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.common.BackfillRecoveryStrategies;
import io.github.lakehouseflow.common.BackfillScopeTypes;
import io.github.lakehouseflow.common.BackfillSkipPolicies;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests BackfillQueryService public methods for scheduler-side audit views.
 */
@ExtendWith(MockitoExtension.class)
class BackfillQueryServiceTest {

    private static final LocalDate START_DATE = LocalDate.of(2026, 9, 10);
    private static final LocalDate END_DATE = LocalDate.of(2026, 9, 12);
    private static final LocalDateTime CREATED_AT = LocalDateTime.of(2026, 9, 12, 21, 0);

    @Mock
    private BackfillBatchRepository backfillBatchRepository;

    @Mock
    private BackfillItemRepository backfillItemRepository;

    @InjectMocks
    private BackfillQueryService backfillQueryService;

    /**
     * Verify action-key lookup maps a persisted batch into a read model.
     */
    @Test
    void findBatchByActionKeyReturnsMappedBatch() {
        when(backfillBatchRepository.findByActionKey("backfill-node-1"))
                .thenReturn(Optional.of(batch()));

        Optional<BackfillQueryService.BackfillBatchView> result =
                backfillQueryService.findBatchByActionKey(" backfill-node-1 ");

        assertTrue(result.isPresent());
        assertEquals(81L, result.get().id());
        assertEquals(BackfillScopeTypes.NODE_SUBGRAPH, result.get().scopeType());
        assertEquals(List.of("node.dwd_orders"), result.get().entryNodeCodes());
        assertEquals(List.of("node.dwd_orders", "node.dm_orders"), result.get().selectedNodeCodes());
        assertEquals("node.dwd_orders", result.get().startNodeCode());
        assertEquals(BackfillProgressionModes.PARALLEL_WITH_LIMIT, result.get().progressionMode());
        assertEquals(2, result.get().maxActiveDates());
        assertEquals(BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES, result.get().skipPolicy());
        assertEquals(1, result.get().skippedDateCount());
        assertTrue(result.get().skipEvidence().containsKey("2026-09-10"));
        assertEquals(80L, result.get().sourceBackfillBatchId());
        assertEquals(1, result.get().recoveryAttempt());
        assertEquals(BackfillRecoveryStrategies.FULL_SCOPE, result.get().recoveryStrategy());
        assertEquals(3, result.get().producedWorkflowCount());
        assertEquals(6, result.get().totalItemCount());
    }

    /**
     * Verify blank action-key lookup returns empty without touching storage.
     */
    @Test
    void findBatchByActionKeyReturnsEmptyForBlankKey() {
        Optional<BackfillQueryService.BackfillBatchView> result =
                backfillQueryService.findBatchByActionKey(" ");

        assertTrue(result.isEmpty());
        verify(backfillBatchRepository, never()).findByActionKey(" ");
    }

    /**
     * Verify batch item lookup maps persisted node/date items in repository order.
     */
    @Test
    void findBatchItemsReturnsMappedItems() {
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(List.of(item(91L, "node.dwd_orders"), item(92L, "node.dm_orders")));

        List<BackfillQueryService.BackfillItemView> result = backfillQueryService.findBatchItems(81L);

        assertEquals(2, result.size());
        assertEquals("node.dwd_orders", result.get(0).nodeCode());
        assertEquals(72L, result.get(1).taskInstanceId());
        assertEquals(BackfillItemStatuses.INTENT_READY, result.get(1).status());
    }

    /**
     * Verify invalid batch ids return an empty list without touching storage.
     */
    @Test
    void findBatchItemsReturnsEmptyForInvalidBatchId() {
        List<BackfillQueryService.BackfillItemView> result = backfillQueryService.findBatchItems(0L);

        assertTrue(result.isEmpty());
        verify(backfillItemRepository, never()).findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(0L);
    }

    /**
     * Build a persisted backfill batch fixture.
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
                .bizDateStart(START_DATE)
                .bizDateEnd(END_DATE)
                .cascadePolicy("DIRECT_DOWNSTREAM")
                .progressionMode(BackfillProgressionModes.PARALLEL_WITH_LIMIT)
                .maxActiveDates(2)
                .skipPolicy(BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES)
                .skippedDateCount(1)
                .skipEvidenceJson(java.util.Map.of("2026-09-10", java.util.Map.of("node.dwd_orders", 71L)))
                .sourceBackfillBatchId(80L)
                .recoveryAttempt(1)
                .recoveryStrategy(BackfillRecoveryStrategies.FULL_SCOPE)
                .status(BackfillBatchStatuses.EXPANDED)
                .producedWorkflowCount(3)
                .totalItemCount(6)
                .requestedBy("alice")
                .reason("repair partitions")
                .createdAt(CREATED_AT)
                .updatedAt(CREATED_AT.plusMinutes(1))
                .build();
    }

    /**
     * Build a persisted backfill item fixture.
     *
     * @param id item id
     * @param nodeCode node code
     * @return backfill item fixture
     */
    private BackfillItem item(Long id, String nodeCode) {
        return BackfillItem.builder()
                .id(id)
                .backfillBatchId(81L)
                .bizDate(START_DATE)
                .flowPlanVersionId(51L)
                .scheduleNodeId(61L)
                .nodeCode(nodeCode)
                .targetAssetKey("paimon.prod.dwd_orders")
                .workflowInstanceId(41L)
                .taskInstanceId(id - 20)
                .status(BackfillItemStatuses.INTENT_READY)
                .createdAt(CREATED_AT)
                .updatedAt(CREATED_AT)
                .build();
    }
}
