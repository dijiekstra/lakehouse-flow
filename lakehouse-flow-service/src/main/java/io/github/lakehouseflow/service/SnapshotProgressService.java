package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotIds;
import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EvaluationResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Reads asset-level snapshot progress and captures scheduling baselines.
 *
 * Asset-level advancement is useful for dependency evaluation and diagnostics,
 * but it cannot attribute a shared-table write to one scheduling intent. Task
 * confirmation therefore uses {@link SnapshotEvidenceService}.
 */
@Service
@RequiredArgsConstructor
public class SnapshotProgressService {

    private final AssetStateRepository assetStateRepository;

    /**
     * Evaluate whether an asset advanced beyond a baseline snapshot.
     *
     * This method does not prove which scheduling intent caused the advancement
     * and must not be used as task-level completion evidence.
     *
     * @param targetAssetKey target asset to inspect
     * @param baselineSnapshotId snapshot observed before scheduling; blank means any target snapshot is progress
     * @return satisfied when the latest target snapshot is greater than baselineSnapshotId
     */
    @Transactional(readOnly = true)
    public EvaluationResult evaluateProgress(String targetAssetKey, String baselineSnapshotId) {
        if (isBlank(targetAssetKey)) {
            return EvaluationResult.unsatisfied(
                    "Target asset key is required",
                    "Cannot confirm snapshot progress without a target asset");
        }

        Optional<AssetState> state = assetStateRepository.findByAssetKey(targetAssetKey);
        if (state.isEmpty()) {
            return EvaluationResult.builder()
                    .satisfied(false)
                    .assetKey(targetAssetKey)
                    .waitingReason("Waiting for target snapshot to advance (asset not found)")
                    .description("Target asset has no AssetState yet")
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        String observedSnapshotId = state.get().getLatestSnapshotId();
        if (isBlank(observedSnapshotId)) {
            return EvaluationResult.builder()
                    .satisfied(false)
                    .assetKey(targetAssetKey)
                    .waitingReason("Waiting for target snapshot to advance (no snapshot yet)")
                    .description("Target AssetState exists but latestSnapshotId is empty")
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        if (isBlank(baselineSnapshotId)) {
            return EvaluationResult.builder()
                    .satisfied(true)
                    .assetKey(targetAssetKey)
                    .snapshotId(observedSnapshotId)
                    .description("Target snapshot " + observedSnapshotId + " exists with no previous baseline")
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        if (SnapshotIds.isAfter(observedSnapshotId, baselineSnapshotId)) {
            return EvaluationResult.builder()
                    .satisfied(true)
                    .assetKey(targetAssetKey)
                    .snapshotId(observedSnapshotId)
                    .description("Target snapshot advanced from " + baselineSnapshotId + " to " + observedSnapshotId)
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        return EvaluationResult.builder()
                .satisfied(false)
                .assetKey(targetAssetKey)
                .snapshotId(observedSnapshotId)
                .waitingReason("Waiting for target snapshot > " + baselineSnapshotId
                        + " (current: " + observedSnapshotId + ")")
                .description("Target snapshot has not advanced beyond baseline")
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Read the latest known snapshot for a target asset.
     *
     * This is used to capture a baseline before Lakehouse Flow emits a
     * scheduling intent. The value is evidence only; missing state simply means
     * the first future target snapshot can confirm progress.
     */
    @Transactional(readOnly = true)
    public Optional<String> findLatestSnapshotId(String targetAssetKey) {
        if (isBlank(targetAssetKey)) {
            return Optional.empty();
        }

        return assetStateRepository.findByAssetKey(targetAssetKey)
                .map(AssetState::getLatestSnapshotId)
                .filter(snapshotId -> !isBlank(snapshotId));
    }

    /**
     * Check whether a string is null or only whitespace.
     *
     * @param value source value
     * @return true when value has no meaningful text
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
