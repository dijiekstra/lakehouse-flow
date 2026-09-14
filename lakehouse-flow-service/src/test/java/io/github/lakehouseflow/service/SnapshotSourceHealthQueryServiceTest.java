package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotSourceHealthOutcomes;
import io.github.lakehouseflow.dao.SnapshotSourceHealthRepository;
import io.github.lakehouseflow.model.SnapshotSourceHealth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests bounded persisted source-health operations queries.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotSourceHealthQueryServiceTest {

    @Mock
    private SnapshotSourceHealthRepository snapshotSourceHealthRepository;

    @InjectMocks
    private SnapshotSourceHealthQueryService queryService;

    /** Verify filters normalize and persisted BLOCKED maps to external SOURCE_BLOCKED. */
    @Test
    void findLatestEvidenceNormalizesFiltersAndResult() {
        SnapshotSourceHealth health = SnapshotSourceHealth.builder()
                .id(21L)
                .sourceType("PAIMON")
                .sourceName("orders")
                .tableAssetKey("lake.ods.orders")
                .outcome(SnapshotSourceHealthOutcomes.BLOCKED)
                .offsetStatus("RETENTION_GAP")
                .projectionStatus("CONSISTENT")
                .durableOffset("10")
                .latestSourceOffset("20")
                .evidenceCheckedAt(LocalDateTime.of(2026, 9, 14, 10, 0))
                .detail("source history is incomplete")
                .updatedAt(LocalDateTime.of(2026, 9, 14, 10, 1))
                .build();
        when(snapshotSourceHealthRepository.findLatestEvidence(
                "PAIMON",
                "orders",
                "flow.orders",
                "lake.ods.orders",
                SnapshotSourceHealthOutcomes.BLOCKED,
                PageRequest.of(0, 25)))
                .thenReturn(List.of(health));

        List<SnapshotSourceHealthQueryService.SnapshotSourceEvidence> result =
                queryService.findLatestEvidence(
                        " paimon ",
                        " orders ",
                        " flow.orders ",
                        "lake.ods.orders.dt=2026-09-14",
                        "source_blocked",
                        25);

        assertEquals(1, result.size());
        assertEquals(SnapshotSourceHealthOutcomes.SOURCE_BLOCKED, result.get(0).sourceHealth());
        assertEquals("RETENTION_GAP", result.get(0).offsetStatus());
        verify(snapshotSourceHealthRepository).findLatestEvidence(
                "PAIMON",
                "orders",
                "flow.orders",
                "lake.ods.orders",
                SnapshotSourceHealthOutcomes.BLOCKED,
                PageRequest.of(0, 25));
    }

    /** Verify unsupported health, invalid assets, and unbounded result requests fail early. */
    @Test
    void findLatestEvidenceRejectsInvalidFilters() {
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findLatestEvidence(null, null, null, null, "UNKNOWN", 10));
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findLatestEvidence(null, null, null, "not-a-table", null, 10));
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findLatestEvidence(null, null, null, null, null, 0));
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findLatestEvidence(null, null, null, null, null, 501));
    }
}
