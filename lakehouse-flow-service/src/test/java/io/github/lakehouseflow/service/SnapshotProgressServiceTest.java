package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EvaluationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests snapshot progress evaluation against AssetState evidence.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotProgressServiceTest {

    @Mock
    private AssetStateRepository assetStateRepository;

    @InjectMocks
    private SnapshotProgressService snapshotProgressService;

    /**
     * Verify progress is satisfied when the observed snapshot is after the baseline.
     */
    @Test
    void confirmsWhenObservedSnapshotAdvancedAfterBaselineSnapshot() {
        when(assetStateRepository.findByAssetKey("paimon.prod.dwd_orders"))
                .thenReturn(Optional.of(assetState("paimon.prod.dwd_orders", "100")));

        EvaluationResult result = snapshotProgressService.evaluateProgress("paimon.prod.dwd_orders", "99");

        assertTrue(result.getSatisfied());
        assertEquals("100", result.getSnapshotId());
    }

    /**
     * Verify progress waits when the observed snapshot equals the baseline.
     */
    @Test
    void waitsWhenObservedSnapshotHasNotAdvancedAfterBaselineSnapshot() {
        when(assetStateRepository.findByAssetKey("paimon.prod.dwd_orders"))
                .thenReturn(Optional.of(assetState("paimon.prod.dwd_orders", "100")));

        EvaluationResult result = snapshotProgressService.evaluateProgress("paimon.prod.dwd_orders", "100");

        assertFalse(result.getSatisfied());
        assertEquals("100", result.getSnapshotId());
        assertTrue(result.getWaitingReason().contains("current: 100"));
    }

    /**
     * Verify missing target asset state becomes an explicit waiting result.
     */
    @Test
    void waitsWhenTargetAssetStateDoesNotExist() {
        when(assetStateRepository.findByAssetKey("paimon.prod.dwd_orders"))
                .thenReturn(Optional.empty());

        EvaluationResult result = snapshotProgressService.evaluateProgress("paimon.prod.dwd_orders", "100");

        assertFalse(result.getSatisfied());
        assertEquals("paimon.prod.dwd_orders", result.getAssetKey());
        assertTrue(result.getWaitingReason().contains("asset not found"));
    }

    /**
     * Verify a first observed snapshot confirms progress when no baseline exists.
     */
    @Test
    void treatsFirstObservedSnapshotAsProgressWhenBaselineIsMissing() {
        when(assetStateRepository.findByAssetKey("paimon.prod.dwd_orders"))
                .thenReturn(Optional.of(assetState("paimon.prod.dwd_orders", "1")));

        EvaluationResult result = snapshotProgressService.evaluateProgress("paimon.prod.dwd_orders", null);

        assertTrue(result.getSatisfied());
        assertEquals("1", result.getSnapshotId());
    }

    /**
     * Verify baseline lookup returns the latest target snapshot.
     */
    @Test
    void readsLatestSnapshotAsSchedulingBaseline() {
        when(assetStateRepository.findByAssetKey("paimon.prod.dwd_orders"))
                .thenReturn(Optional.of(assetState("paimon.prod.dwd_orders", "100")));

        Optional<String> snapshotId = snapshotProgressService.findLatestSnapshotId("paimon.prod.dwd_orders");

        assertTrue(snapshotId.isPresent());
        assertEquals("100", snapshotId.get());
    }

    /** Verify a compaction after the data baseline cannot satisfy generic data progress. */
    @Test
    void ignoresObservedMaintenanceProgressButUsesItAsNextBaseline() {
        when(assetStateRepository.findByAssetKey("paimon.prod.dwd_orders"))
                .thenReturn(Optional.of(assetState("paimon.prod.dwd_orders", "101", "100")));

        EvaluationResult result = snapshotProgressService.evaluateProgress("paimon.prod.dwd_orders", "100");
        Optional<String> baseline = snapshotProgressService.findLatestSnapshotId("paimon.prod.dwd_orders");

        assertFalse(result.getSatisfied());
        assertEquals("100", result.getSnapshotId());
        assertEquals("101", baseline.orElseThrow());
    }

    /**
     * Build an AssetState fixture with a supplied latest snapshot.
     */
    private AssetState assetState(String assetKey, String snapshotId) {
        return assetState(assetKey, snapshotId, snapshotId);
    }

    /** Build an AssetState fixture with independent observed and data coordinates. */
    private AssetState assetState(String assetKey, String observedSnapshotId, String dataSnapshotId) {
        return AssetState.builder()
                .assetKey(assetKey)
                .assetType("TABLE")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("dwd_orders")
                .latestSnapshotId(observedSnapshotId)
                .latestDataSnapshotId(dataSnapshotId)
                .build();
    }
}
