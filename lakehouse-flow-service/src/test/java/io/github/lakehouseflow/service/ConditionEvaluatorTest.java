package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EvaluationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests ConditionEvaluator public evaluation entry point for supported conditions.
 */
@ExtendWith(MockitoExtension.class)
class ConditionEvaluatorTest {

    private static final String ASSET_KEY = "paimon.prod.orders";

    @Mock
    private AssetStateRepository assetStateRepository;

    @InjectMocks
    private ConditionEvaluator conditionEvaluator;

    /**
     * Verify SNAPSHOT_EXISTS succeeds when the asset has a latest snapshot.
     */
    @Test
    void evaluateConditionSnapshotExistsSucceeds() {
        when(assetStateRepository.findByAssetKey(ASSET_KEY)).thenReturn(Optional.of(assetState()));

        EvaluationResult result = conditionEvaluator.evaluateCondition("SNAPSHOT_EXISTS", ASSET_KEY, null);

        assertTrue(result.getSatisfied());
        assertEquals("100", result.getSnapshotId());
    }

    /**
     * Verify SNAPSHOT_EXISTS waits when the asset state is missing.
     */
    @Test
    void evaluateConditionSnapshotExistsWaitsWhenAssetMissing() {
        when(assetStateRepository.findByAssetKey(ASSET_KEY)).thenReturn(Optional.empty());

        EvaluationResult result = conditionEvaluator.evaluateCondition("SNAPSHOT_EXISTS", ASSET_KEY, null);

        assertFalse(result.getSatisfied());
        assertTrue(result.getWaitingReason().contains("not found"));
    }

    /**
     * Verify SNAPSHOT_ID_GTE compares snapshot ids with numeric semantics.
     */
    @Test
    void evaluateConditionSnapshotIdGteSucceeds() {
        when(assetStateRepository.findByAssetKey(ASSET_KEY)).thenReturn(Optional.of(assetState()));

        EvaluationResult result = conditionEvaluator.evaluateCondition("SNAPSHOT_ID_GTE", ASSET_KEY, "99");

        assertTrue(result.getSatisfied());
        assertEquals("100", result.getSnapshotId());
    }

    /**
     * Verify WATERMARK_GTE succeeds for equal or later watermarks.
     */
    @Test
    void evaluateConditionWatermarkGteSucceeds() {
        when(assetStateRepository.findByAssetKey(ASSET_KEY)).thenReturn(Optional.of(assetState()));

        EvaluationResult result = conditionEvaluator.evaluateCondition(
                "WATERMARK_GTE",
                ASSET_KEY,
                "2026-09-12T00:30:00");

        assertTrue(result.getSatisfied());
        assertEquals("2026-09-12T01:00:00", result.getWatermark());
    }

    /**
     * Verify QUALITY_PASSED checks asset quality evidence.
     */
    @Test
    void evaluateConditionQualityPassedSucceeds() {
        when(assetStateRepository.findByAssetKey(ASSET_KEY)).thenReturn(Optional.of(assetState()));

        EvaluationResult result = conditionEvaluator.evaluateCondition("QUALITY_PASSED", ASSET_KEY, null);

        assertTrue(result.getSatisfied());
    }

    /**
     * Verify SCHEMA_COMPATIBLE checks schema evidence.
     */
    @Test
    void evaluateConditionSchemaCompatibleSucceeds() {
        when(assetStateRepository.findByAssetKey(ASSET_KEY)).thenReturn(Optional.of(assetState()));

        EvaluationResult result = conditionEvaluator.evaluateCondition("SCHEMA_COMPATIBLE", ASSET_KEY, null);

        assertTrue(result.getSatisfied());
    }

    /**
     * Verify unsupported condition types are explicit waits.
     */
    @Test
    void evaluateConditionUnknownTypeIsUnsatisfied() {
        EvaluationResult result = conditionEvaluator.evaluateCondition("UNKNOWN_TYPE", ASSET_KEY, null);

        assertFalse(result.getSatisfied());
        assertTrue(result.getWaitingReason().contains("Unknown condition type"));
    }

    /**
     * Build an AssetState fixture with complete condition evidence.
     */
    private AssetState assetState() {
        return AssetState.builder()
                .assetKey(ASSET_KEY)
                .assetType("TABLE")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .latestSnapshotId("100")
                .latestWatermark(LocalDateTime.of(2026, 9, 12, 1, 0))
                .qualityStatus("PASSED")
                .schemaStatus("COMPATIBLE")
                .build();
    }
}
