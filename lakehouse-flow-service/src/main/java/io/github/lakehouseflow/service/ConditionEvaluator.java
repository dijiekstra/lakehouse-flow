package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotIds;
import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EvaluationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * Condition Evaluator - Evaluates dependency conditions for workflows.
 *
 * Supported condition types:
 * - SNAPSHOT_EXISTS: Asset has a business-data snapshot
 * - SNAPSHOT_ID_GTE: Latest business-data snapshot ID >= required value
 * - WATERMARK_GTE: Latest business-data watermark >= required time
 * - QUALITY_PASSED: Quality status is PASSED
 * - SCHEMA_COMPATIBLE: Schema status is COMPATIBLE
 *
 * Returns EvaluationResult with:
 * - satisfied: true if condition is met
 * - waitingReason: human-readable reason if not satisfied
 * - description: audit log of evaluation
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ConditionEvaluator {

    private final AssetStateRepository assetStateRepository;

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /**
     * Evaluate a single dependency condition.
     *
     * @param conditionType Type of condition (SNAPSHOT_EXISTS, SNAPSHOT_ID_GTE, etc.)
     * @param assetKey Asset to check
     * @param requiredValue Required value (for comparisons)
     * @return EvaluationResult with satisfaction status
     */
    public EvaluationResult evaluateCondition(
            String conditionType,
            String assetKey,
            String requiredValue) {

        return switch (conditionType) {
            case "SNAPSHOT_EXISTS" -> checkSnapshotExists(assetKey);
            case "SNAPSHOT_ID_GTE" -> checkSnapshotIdGte(assetKey, requiredValue);
            case "WATERMARK_GTE" -> checkWatermarkGte(assetKey, requiredValue);
            case "QUALITY_PASSED" -> checkQualityStatus(assetKey);
            case "SCHEMA_COMPATIBLE" -> checkSchemaStatus(assetKey);
            default -> EvaluationResult.unsatisfied("Unknown condition type: " + conditionType);
        };
    }

    /**
     * Check if asset has any snapshot.
     */
    private EvaluationResult checkSnapshotExists(String assetKey) {
        Optional<AssetState> state = assetStateRepository.findByAssetKey(assetKey);

        if (state.isEmpty()) {
            return EvaluationResult.unsatisfied(
                    "Asset " + assetKey + " not found",
                    "Asset has no snapshot yet");
        }

        if (state.get().getLatestDataSnapshotId() == null) {
            return EvaluationResult.unsatisfied(
                    "Asset " + assetKey + " has no data snapshot",
                    "Asset state exists but latestDataSnapshotId is null");
        }

        String snapshotId = state.get().getLatestDataSnapshotId();
        return EvaluationResult.builder()
                .satisfied(true)
                .description("Snapshot " + snapshotId + " exists")
                .assetKey(assetKey)
                .snapshotId(snapshotId)
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Check if latest snapshot ID >= required value.
     * E.g., snapshot "1000" >= "950" → true
     */
    private EvaluationResult checkSnapshotIdGte(String assetKey, String requiredValue) {
        Optional<AssetState> state = assetStateRepository.findByAssetKey(assetKey);

        if (state.isEmpty() || state.get().getLatestDataSnapshotId() == null) {
            return EvaluationResult.unsatisfied(
                    "Waiting for snapshot >= " + requiredValue + " (asset not found)",
                    "Asset " + assetKey + " has no snapshot");
        }

        String currentSnapshotId = state.get().getLatestDataSnapshotId();

        if (SnapshotIds.isGreaterThanOrEqual(currentSnapshotId, requiredValue)) {
            return EvaluationResult.builder()
                    .satisfied(true)
                    .description("Snapshot " + currentSnapshotId + " >= required " + requiredValue)
                    .assetKey(assetKey)
                    .snapshotId(currentSnapshotId)
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        return EvaluationResult.unsatisfied(
                "Waiting for snapshot >= " + requiredValue + " (current: " + currentSnapshotId + ")",
                "Snapshot ID comparison failed: " + currentSnapshotId + " < " + requiredValue);
    }

    /**
     * Check if latest watermark >= required time.
     * E.g., watermark "2026-09-12T00:30" >= "2026-09-11T23:59" → true
     */
    private EvaluationResult checkWatermarkGte(String assetKey, String requiredWatermarkStr) {
        Optional<AssetState> state = assetStateRepository.findByAssetKey(assetKey);

        if (state.isEmpty() || state.get().getLatestDataWatermark() == null) {
            return EvaluationResult.unsatisfied(
                    "Waiting for watermark >= " + requiredWatermarkStr + " (asset not found)",
                    "Asset " + assetKey + " has no watermark");
        }

        LocalDateTime currentWatermark = state.get().getLatestDataWatermark();

        try {
            LocalDateTime requiredWatermark = LocalDateTime.parse(requiredWatermarkStr, ISO_FORMATTER);

            if (currentWatermark.isAfter(requiredWatermark) || currentWatermark.isEqual(requiredWatermark)) {
                String formattedWatermark = currentWatermark.format(ISO_FORMATTER);
                return EvaluationResult.builder()
                        .satisfied(true)
                        .description("Watermark " + formattedWatermark + " >= required " + requiredWatermarkStr)
                        .assetKey(assetKey)
                        .watermark(formattedWatermark)
                        .evaluatedAt(System.currentTimeMillis())
                        .build();
            }

            String formattedCurrent = currentWatermark.format(ISO_FORMATTER);
            return EvaluationResult.unsatisfied(
                    "Waiting for watermark >= " + requiredWatermarkStr + " (current: " + formattedCurrent + ")",
                    "Watermark check failed: " + formattedCurrent + " < " + requiredWatermarkStr);

        } catch (Exception e) {
            log.error("Failed to parse watermark: {}", requiredWatermarkStr, e);
            return EvaluationResult.unsatisfied(
                    "Invalid watermark format: " + requiredWatermarkStr,
                    "Failed to parse required watermark: " + e.getMessage());
        }
    }

    /**
     * Check if quality status is PASSED.
     */
    private EvaluationResult checkQualityStatus(String assetKey) {
        Optional<AssetState> state = assetStateRepository.findByAssetKey(assetKey);

        if (state.isEmpty()) {
            return EvaluationResult.unsatisfied(
                    "Waiting for quality check (asset not found)",
                    "Asset " + assetKey + " not found");
        }

        String qualityStatus = state.get().getQualityStatus();

        if ("PASSED".equals(qualityStatus)) {
            return EvaluationResult.builder()
                    .satisfied(true)
                    .description("Quality check PASSED")
                    .assetKey(assetKey)
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        return EvaluationResult.unsatisfied(
                "Waiting for quality check (current: " + qualityStatus + ")",
                "Quality status is not PASSED");
    }

    /**
     * Check if schema status is COMPATIBLE.
     */
    private EvaluationResult checkSchemaStatus(String assetKey) {
        Optional<AssetState> state = assetStateRepository.findByAssetKey(assetKey);

        if (state.isEmpty()) {
            return EvaluationResult.unsatisfied(
                    "Waiting for schema check (asset not found)",
                    "Asset " + assetKey + " not found");
        }

        String schemaStatus = state.get().getSchemaStatus();

        if ("COMPATIBLE".equals(schemaStatus)) {
            return EvaluationResult.builder()
                    .satisfied(true)
                    .description("Schema COMPATIBLE")
                    .assetKey(assetKey)
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        return EvaluationResult.unsatisfied(
                "Waiting for schema compatibility (current: " + schemaStatus + ")",
                "Schema status is not COMPATIBLE");
    }
}
