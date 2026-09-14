package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.OperationalBlockerResponse;
import io.github.lakehouseflow.api.dto.SnapshotSourceEvidenceResponse;
import io.github.lakehouseflow.service.SchedulingBlockerQueryService;
import io.github.lakehouseflow.service.SnapshotSourceHealthQueryService;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the minimal read-only operations API mappings.
 */
class OperationsControllerTest {

    /** Verify unified blockers expose only scheduler-owned evidence fields. */
    @Test
    void findBlockersMapsOperationalEvidence() {
        SchedulingBlockerQueryService blockerQueryService = mock(SchedulingBlockerQueryService.class);
        SnapshotSourceHealthQueryService sourceQueryService = mock(SnapshotSourceHealthQueryService.class);
        OperationsController controller = new OperationsController(blockerQueryService, sourceQueryService);
        LocalDateTime observedAt = LocalDateTime.of(2026, 9, 14, 10, 0);
        when(blockerQueryService.findBlockers("SOURCE_BLOCKED", "flow.orders", null, 25))
                .thenReturn(List.of(new SchedulingBlockerQueryService.OperationalBlocker(
                        "SOURCE_BLOCKED",
                        "TASK_INSTANCE",
                        11L,
                        "1:node.dwd",
                        "flow.orders",
                        "node.dwd",
                        null,
                        "lake.dwd.orders",
                        observedAt.toLocalDate().atStartOfDay(),
                        "SCHEDULED",
                        null,
                        "SOURCE_BLOCKED",
                        "RETENTION_GAP",
                        observedAt)));

        List<OperationalBlockerResponse> result =
                controller.findBlockers("SOURCE_BLOCKED", "flow.orders", null, 25);

        assertEquals(1, result.size());
        assertEquals("TASK_INSTANCE", result.get(0).subjectType());
        assertEquals("RETENTION_GAP", result.get(0).reason());
    }

    /** Verify persisted source reconciliation fields map into the external source vocabulary. */
    @Test
    void findSnapshotSourcesMapsReconciliationEvidence() {
        SchedulingBlockerQueryService blockerQueryService = mock(SchedulingBlockerQueryService.class);
        SnapshotSourceHealthQueryService sourceQueryService = mock(SnapshotSourceHealthQueryService.class);
        OperationsController controller = new OperationsController(blockerQueryService, sourceQueryService);
        LocalDateTime checkedAt = LocalDateTime.of(2026, 9, 14, 10, 0);
        when(sourceQueryService.findLatestEvidence(
                "PAIMON", "orders", "flow.orders", null, "SOURCE_BLOCKED", 25))
                .thenReturn(List.of(new SnapshotSourceHealthQueryService.SnapshotSourceEvidence(
                        21L,
                        "PAIMON",
                        "orders",
                        "lake.ods.orders",
                        "SOURCE_BLOCKED",
                        "RETENTION_GAP",
                        "CONSISTENT",
                        "10",
                        "20",
                        checkedAt,
                        "source history is incomplete",
                        checkedAt.plusSeconds(1))));

        List<SnapshotSourceEvidenceResponse> result = controller.findSnapshotSources(
                "PAIMON", "orders", "flow.orders", null, "SOURCE_BLOCKED", 25);

        assertEquals(1, result.size());
        assertEquals("SOURCE_BLOCKED", result.get(0).sourceHealth());
        assertEquals("RETENTION_GAP", result.get(0).offsetStatus());
    }
}
