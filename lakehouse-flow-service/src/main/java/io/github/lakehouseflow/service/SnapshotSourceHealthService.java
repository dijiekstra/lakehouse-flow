package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.AssetKeys;
import io.github.lakehouseflow.common.SnapshotSourceHealthOutcomes;
import io.github.lakehouseflow.dao.SnapshotSourceHealthRepository;
import io.github.lakehouseflow.model.SnapshotSourceHealth;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * Decides whether persisted source evidence can safely close a snapshot confirmation window.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SnapshotSourceHealthService {

    private final SnapshotSourceHealthRepository snapshotSourceHealthRepository;

    /**
     * Evaluate source evidence for a table or partition target at one confirmation deadline.
     *
     * @param targetAssetKey target table or partition asset
     * @param confirmationDeadline end of the snapshot confirmation window
     * @return source status and whether SNAPSHOT_NOT_ADVANCED is now trustworthy
     */
    public SourceHealthDecision evaluateTimeoutEvidence(
            String targetAssetKey,
            LocalDateTime confirmationDeadline) {
        if (confirmationDeadline == null) {
            throw new IllegalArgumentException("confirmationDeadline must not be null");
        }
        Optional<String> tableAssetKey = AssetKeys.tableKey(targetAssetKey);
        if (tableAssetKey.isEmpty()) {
            return SourceHealthDecision.blocked(
                    null,
                    "UNMANAGED_TARGET_SOURCE: invalid catalog.database.table[.partition] target",
                    null);
        }
        Optional<SnapshotSourceHealth> health = snapshotSourceHealthRepository
                .findByTableAssetKey(tableAssetKey.get());
        if (health.isEmpty()) {
            return SourceHealthDecision.blocked(
                    tableAssetKey.get(),
                    "UNMANAGED_TARGET_SOURCE: no persisted source health for target table",
                    null);
        }

        SnapshotSourceHealth proof = health.get();
        if (!SnapshotSourceHealthOutcomes.HEALTHY.equals(proof.getOutcome())) {
            String status = SnapshotSourceHealthOutcomes.REPAIRABLE.equals(proof.getOutcome())
                    ? SnapshotSourceHealthOutcomes.REPAIRABLE
                    : SnapshotSourceHealthOutcomes.SOURCE_BLOCKED;
            return new SourceHealthDecision(
                    tableAssetKey.get(),
                    status,
                    false,
                    proof.getDetail(),
                    proof.getEvidenceCheckedAt());
        }
        if (proof.getEvidenceCheckedAt() == null
                || proof.getEvidenceCheckedAt().isBefore(confirmationDeadline)) {
            return SourceHealthDecision.blocked(
                    tableAssetKey.get(),
                    "STALE_SOURCE_HEALTH: source was not checked after the confirmation deadline",
                    proof.getEvidenceCheckedAt());
        }
        if (!Objects.equals(proof.getDurableOffset(), proof.getLatestSourceOffset())) {
            return SourceHealthDecision.blocked(
                    tableAssetKey.get(),
                    "SOURCE_OFFSET_NOT_CAUGHT_UP: durable offset does not equal latest source offset",
                    proof.getEvidenceCheckedAt());
        }
        return new SourceHealthDecision(
                tableAssetKey.get(),
                SnapshotSourceHealthOutcomes.HEALTHY,
                true,
                proof.getDetail(),
                proof.getEvidenceCheckedAt());
    }

    /**
     * Immutable source gate evaluated independently from task snapshot and delivery status.
     *
     * @param tableAssetKey normalized managed table key, or null for an invalid target
     * @param sourceHealth externally visible source status
     * @param timeoutConclusionAllowed whether the scheduler may conclude SNAPSHOT_NOT_ADVANCED
     * @param detail source reconciliation or blocking detail
     * @param evidenceCheckedAt time the source was actually checked, or null
     */
    public record SourceHealthDecision(
            String tableAssetKey,
            String sourceHealth,
            boolean timeoutConclusionAllowed,
            String detail,
            LocalDateTime evidenceCheckedAt) {

        /** Build a fail-closed source decision. */
        private static SourceHealthDecision blocked(
                String tableAssetKey,
                String detail,
                LocalDateTime evidenceCheckedAt) {
            return new SourceHealthDecision(
                    tableAssetKey,
                    SnapshotSourceHealthOutcomes.SOURCE_BLOCKED,
                    false,
                    detail,
                    evidenceCheckedAt);
        }
    }
}
