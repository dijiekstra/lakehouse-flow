package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.BackfillBatchResponse;
import io.github.lakehouseflow.api.dto.BackfillItemResponse;
import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.common.BackfillRecoveryStrategies;
import io.github.lakehouseflow.common.BackfillScopeTypes;
import io.github.lakehouseflow.common.BackfillSkipPolicies;
import io.github.lakehouseflow.service.BackfillQueryService;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Tests BackfillController mapping for backfill audit query APIs.
 */
@ExtendWith(MockitoExtension.class)
class BackfillControllerTest {

    private static final LocalDate START_DATE = LocalDate.of(2026, 9, 10);
    private static final LocalDateTime CREATED_AT = LocalDateTime.of(2026, 9, 12, 21, 0);

    @Mock
    private BackfillQueryService backfillQueryService;

    /**
     * Verify action-key batch lookup returns an API response.
     */
    @Test
    void getBatchByActionKeyReturnsBatchResponse() {
        BackfillController controller = new BackfillController(backfillQueryService);
        when(backfillQueryService.findBatchByActionKey("backfill-node-1"))
                .thenReturn(Optional.of(batchView()));

        ResponseEntity<BackfillBatchResponse> response = controller.getBatchByActionKey("backfill-node-1");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(81L, response.getBody().id());
        assertEquals(BackfillBatchStatuses.EXPANDED, response.getBody().status());
        assertEquals(BackfillScopeTypes.NODE_SUBGRAPH, response.getBody().scopeType());
        assertEquals(List.of("node.dwd_orders"), response.getBody().entryNodeCodes());
        assertEquals(List.of("node.dwd_orders", "node.dm_orders"), response.getBody().selectedNodeCodes());
        assertEquals(BackfillProgressionModes.PARALLEL_WITH_LIMIT, response.getBody().progressionMode());
        assertEquals(2, response.getBody().maxActiveDates());
        assertEquals(BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES, response.getBody().skipPolicy());
        assertEquals(1, response.getBody().skippedDateCount());
        assertEquals(80L, response.getBody().sourceBackfillBatchId());
        assertEquals(1, response.getBody().recoveryAttempt());
        assertEquals(BackfillRecoveryStrategies.FULL_SCOPE, response.getBody().recoveryStrategy());
    }

    /**
     * Verify missing action-key batch lookup returns 404.
     */
    @Test
    void getBatchByActionKeyReturnsNotFoundWhenMissing() {
        BackfillController controller = new BackfillController(backfillQueryService);
        when(backfillQueryService.findBatchByActionKey("missing")).thenReturn(Optional.empty());

        ResponseEntity<BackfillBatchResponse> response = controller.getBatchByActionKey("missing");

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    /**
     * Verify item listing maps service views into API responses.
     */
    @Test
    void listBatchItemsReturnsItemResponses() {
        BackfillController controller = new BackfillController(backfillQueryService);
        when(backfillQueryService.findBatchItems(81L)).thenReturn(List.of(itemView()));

        List<BackfillItemResponse> response = controller.listBatchItems(81L);

        assertEquals(1, response.size());
        assertEquals("node.dwd_orders", response.get(0).nodeCode());
        assertEquals(71L, response.get(0).taskInstanceId());
    }

    /**
     * Build a backfill batch service view fixture.
     */
    private BackfillQueryService.BackfillBatchView batchView() {
        return new BackfillQueryService.BackfillBatchView(
                81L,
                "backfill-node-1",
                "backfill-node-1",
                51L,
                "flow.orders",
                4,
                BackfillScopeTypes.NODE_SUBGRAPH,
                List.of("node.dwd_orders"),
                List.of("node.dwd_orders", "node.dm_orders"),
                61L,
                "node.dwd_orders",
                START_DATE,
                START_DATE.plusDays(2),
                "DIRECT_DOWNSTREAM",
                BackfillProgressionModes.PARALLEL_WITH_LIMIT,
                2,
                BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES,
                1,
                java.util.Map.of("2026-09-10", java.util.Map.of("node.dwd_orders", 71L)),
                80L,
                1,
                BackfillRecoveryStrategies.FULL_SCOPE,
                BackfillBatchStatuses.EXPANDED,
                3,
                6,
                "alice",
                "repair partitions",
                CREATED_AT,
                CREATED_AT.plusMinutes(1));
    }

    /**
     * Build a backfill item service view fixture.
     */
    private BackfillQueryService.BackfillItemView itemView() {
        return new BackfillQueryService.BackfillItemView(
                91L,
                81L,
                START_DATE,
                51L,
                61L,
                "node.dwd_orders",
                "paimon.prod.dwd_orders",
                41L,
                71L,
                BackfillItemStatuses.INTENT_READY,
                CREATED_AT,
                CREATED_AT);
    }
}
