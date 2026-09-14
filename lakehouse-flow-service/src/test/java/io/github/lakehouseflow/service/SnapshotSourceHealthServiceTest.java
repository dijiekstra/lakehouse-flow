package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotSourceHealthOutcomes;
import io.github.lakehouseflow.dao.SnapshotSourceHealthRepository;
import io.github.lakehouseflow.model.SnapshotSourceHealth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests the source-health gate used before snapshot timeout conclusions.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotSourceHealthServiceTest {

    private static final LocalDateTime DEADLINE = LocalDateTime.of(2026, 9, 14, 10, 0);
    private static final String TABLE = "lake.dwd.orders";

    @Mock
    private SnapshotSourceHealthRepository snapshotSourceHealthRepository;

    @InjectMocks
    private SnapshotSourceHealthService snapshotSourceHealthService;

    /** Verify a partition target binds to the managed table and accepts fresh caught-up proof. */
    @Test
    void evaluateTimeoutEvidenceAllowsFreshHealthyTableProof() {
        when(snapshotSourceHealthRepository.findByTableAssetKey(TABLE))
                .thenReturn(Optional.of(health(SnapshotSourceHealthOutcomes.HEALTHY, DEADLINE.plusSeconds(1))));

        SnapshotSourceHealthService.SourceHealthDecision result =
                snapshotSourceHealthService.evaluateTimeoutEvidence(TABLE + ".dt=2026-09-13", DEADLINE);

        assertTrue(result.timeoutConclusionAllowed());
        assertEquals(TABLE, result.tableAssetKey());
        assertEquals(SnapshotSourceHealthOutcomes.HEALTHY, result.sourceHealth());
    }

    /** Verify a healthy proof older than the deadline cannot establish a complete confirmation window. */
    @Test
    void evaluateTimeoutEvidenceBlocksStaleHealthyProof() {
        when(snapshotSourceHealthRepository.findByTableAssetKey(TABLE))
                .thenReturn(Optional.of(health(SnapshotSourceHealthOutcomes.HEALTHY, DEADLINE.minusSeconds(1))));

        SnapshotSourceHealthService.SourceHealthDecision result =
                snapshotSourceHealthService.evaluateTimeoutEvidence(TABLE, DEADLINE);

        assertFalse(result.timeoutConclusionAllowed());
        assertEquals(SnapshotSourceHealthOutcomes.SOURCE_BLOCKED, result.sourceHealth());
        assertTrue(result.detail().startsWith("STALE_SOURCE_HEALTH"));
    }

    /** Verify repairable lag remains a separate source result and cannot fail the task. */
    @Test
    void evaluateTimeoutEvidenceKeepsRepairableSourceNonTerminal() {
        SnapshotSourceHealth health = health(SnapshotSourceHealthOutcomes.REPAIRABLE, DEADLINE.plusSeconds(1));
        health.setDetail("offset is lagging");
        when(snapshotSourceHealthRepository.findByTableAssetKey(TABLE)).thenReturn(Optional.of(health));

        SnapshotSourceHealthService.SourceHealthDecision result =
                snapshotSourceHealthService.evaluateTimeoutEvidence(TABLE, DEADLINE);

        assertFalse(result.timeoutConclusionAllowed());
        assertEquals(SnapshotSourceHealthOutcomes.REPAIRABLE, result.sourceHealth());
        assertEquals("offset is lagging", result.detail());
    }

    /** Verify an unmanaged target fails closed instead of inventing source health. */
    @Test
    void evaluateTimeoutEvidenceBlocksUnmanagedTarget() {
        when(snapshotSourceHealthRepository.findByTableAssetKey(TABLE)).thenReturn(Optional.empty());

        SnapshotSourceHealthService.SourceHealthDecision result =
                snapshotSourceHealthService.evaluateTimeoutEvidence(TABLE, DEADLINE);

        assertFalse(result.timeoutConclusionAllowed());
        assertTrue(result.detail().startsWith("UNMANAGED_TARGET_SOURCE"));
    }

    /** Verify the confirmation deadline is required for freshness comparison. */
    @Test
    void evaluateTimeoutEvidenceRejectsMissingDeadline() {
        assertThrows(IllegalArgumentException.class,
                () -> snapshotSourceHealthService.evaluateTimeoutEvidence(TABLE, null));
    }

    /** Build one persisted source proof with matching durable and live offsets. */
    private SnapshotSourceHealth health(String outcome, LocalDateTime checkedAt) {
        return SnapshotSourceHealth.builder()
                .sourceType("PAIMON")
                .sourceName("orders")
                .tableAssetKey(TABLE)
                .outcome(outcome)
                .offsetStatus("IN_SYNC")
                .projectionStatus("CONSISTENT")
                .durableOffset("101")
                .latestSourceOffset("101")
                .evidenceCheckedAt(checkedAt)
                .detail("source caught up")
                .build();
    }
}
