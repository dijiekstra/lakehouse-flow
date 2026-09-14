package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.SchedulingActionDetailResponse;
import io.github.lakehouseflow.api.dto.SchedulingActionSummaryResponse;
import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.common.BackfillScopeTypes;
import io.github.lakehouseflow.common.BackfillSkipPolicies;
import io.github.lakehouseflow.common.SchedulingActionStatuses;
import io.github.lakehouseflow.common.SchedulingActionTypes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.service.SchedulingActionQueryService;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Tests action query controller mapping and HTTP-facing validation behavior.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingActionQueryControllerTest {

    private static final LocalDate BIZ_DATE = LocalDate.of(2026, 9, 10);
    private static final LocalDateTime CREATED_AT = LocalDateTime.of(2026, 9, 13, 10, 0);

    @Mock
    private SchedulingActionQueryService schedulingActionQueryService;

    /**
     * Verify definition-anchor search maps service summaries to API responses.
     */
    @Test
    void searchActionsReturnsMappedSummaries() {
        SchedulingActionQueryController controller = new SchedulingActionQueryController(
                schedulingActionQueryService);
        when(schedulingActionQueryService.searchActions("flow.orders", 51L, 61L, 25))
                .thenReturn(List.of(actionSummary()));

        List<SchedulingActionSummaryResponse> response =
                controller.searchActions("flow.orders", 51L, 61L, 25);

        assertEquals(1, response.size());
        assertEquals("backfill-node-1", response.get(0).actionKey());
        assertEquals(51L, response.get(0).flowPlanVersionId());
    }

    /**
     * Verify invalid service search input is exposed as an HTTP 400 error.
     */
    @Test
    void searchActionsMapsValidationFailureToBadRequest() {
        SchedulingActionQueryController controller = new SchedulingActionQueryController(
                schedulingActionQueryService);
        when(schedulingActionQueryService.searchActions(null, null, null, null))
                .thenThrow(new IllegalArgumentException("At least one filter is required"));

        ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> controller.searchActions(null, null, null, null));

        assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
    }

    /**
     * Verify action-key detail maps batch metadata and snapshot evidence.
     */
    @Test
    void getActionReturnsCompleteEvidenceResponse() {
        SchedulingActionQueryController controller = new SchedulingActionQueryController(
                schedulingActionQueryService);
        when(schedulingActionQueryService.findActionByKey("backfill-node-1"))
                .thenReturn(Optional.of(actionDetail()));

        ResponseEntity<SchedulingActionDetailResponse> response = controller.getAction("backfill-node-1");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(81L, response.getBody().backfillBatch().id());
        assertEquals(1, response.getBody().snapshotEvidence().size());
        assertEquals(Boolean.TRUE, response.getBody().snapshotEvidence().get(0).snapshotAdvanced());
    }

    /**
     * Verify missing action keys produce an HTTP 404 response.
     */
    @Test
    void getActionReturnsNotFoundWhenMissing() {
        SchedulingActionQueryController controller = new SchedulingActionQueryController(
                schedulingActionQueryService);
        when(schedulingActionQueryService.findActionByKey("missing")).thenReturn(Optional.empty());

        ResponseEntity<SchedulingActionDetailResponse> response = controller.getAction("missing");

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    /**
     * Build an action detail service view fixture.
     *
     * @return complete action detail fixture
     */
    private SchedulingActionQueryService.ActionDetailView actionDetail() {
        SchedulingActionQueryService.BackfillBatchEvidenceView batch =
                new SchedulingActionQueryService.BackfillBatchEvidenceView(
                        81L,
                        "backfill-node-1",
                        "backfill-node-1",
                        BackfillScopeTypes.NODE_SUBGRAPH,
                        List.of("node.dwd_orders"),
                        List.of("node.dwd_orders", "node.dm_orders"),
                        61L,
                        "node.dwd_orders",
                        BIZ_DATE,
                        BIZ_DATE.plusDays(1),
                        "ALL_DOWNSTREAM",
                        BackfillProgressionModes.SERIAL,
                        null,
                        BackfillSkipPolicies.NONE,
                        0,
                        Map.of(),
                        null,
                        0,
                        null,
                        BackfillBatchStatuses.EXPANDED,
                        2,
                        4);
        SchedulingActionQueryService.TaskSnapshotEvidenceView task =
                new SchedulingActionQueryService.TaskSnapshotEvidenceView(
                        91L,
                        BackfillItemStatuses.INTENT_DELIVERED,
                        true,
                        71L,
                        41L,
                        51L,
                        61L,
                        "node.dwd_orders",
                        BIZ_DATE.atStartOfDay(),
                        SchedulingStates.SNAPSHOT_CONFIRMED,
                        null,
                        "paimon.prod.dwd_orders",
                        "10",
                        "11",
                        true,
                        CREATED_AT.plusMinutes(5),
                        "HEALTHY",
                        "source caught up",
                        CREATED_AT.plusMinutes(4),
                        CREATED_AT,
                        101L,
                        "task-instance:71",
                        "DATABASE_TABLE",
                        "PUBLISHED",
                        1,
                        null,
                        null,
                        null,
                        CREATED_AT,
                        CREATED_AT.plusMinutes(5));
        return new SchedulingActionQueryService.ActionDetailView(
                actionSummary(),
                null,
                null,
                Map.of("nodeCode", "node.dwd_orders"),
                batch,
                List.of(task));
    }

    /**
     * Build an action summary service view fixture.
     *
     * @return action summary fixture
     */
    private SchedulingActionQueryService.ActionSummaryView actionSummary() {
        return new SchedulingActionQueryService.ActionSummaryView(
                31L,
                "backfill-node-1",
                SchedulingActionTypes.BACKFILL_NODE,
                "SCHEDULE_NODE",
                SchedulingActionStatuses.APPLIED,
                "flow.orders",
                4,
                51L,
                61L,
                null,
                null,
                41L,
                71L,
                BIZ_DATE,
                BIZ_DATE.plusDays(1),
                "alice",
                "repair partitions",
                "Scheduling decision emitted",
                CREATED_AT,
                CREATED_AT.plusMinutes(1));
    }
}
