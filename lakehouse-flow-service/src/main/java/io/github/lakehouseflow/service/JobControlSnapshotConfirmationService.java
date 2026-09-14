package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.JobControlIntentContract;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.JobControlIntent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Confirms platform writer generations solely from target snapshot evidence.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class JobControlSnapshotConfirmationService {

    private final JobControlIntentRepository jobControlIntentRepository;
    private final SnapshotEvidenceService snapshotEvidenceService;
    private final SnapshotSourceHealthService snapshotSourceHealthService;

    /**
     * Check one job-control intent without consulting engine runtime status.
     *
     * @param intentId immutable control intent id
     * @return current snapshot and source evidence result
     */
    public JobControlConfirmationResult checkIntent(Long intentId) {
        JobControlIntent intent = getIntent(intentId);
        if (!JobControlIntentContract.WAITING.equals(intent.getSnapshotResult())) {
            return toResult(intent);
        }
        EvaluationResult evidence = snapshotEvidenceService.evaluateJobControlProgress(intent);
        LocalDateTime now = LocalDateTime.now();
        intent.setObservedSnapshotId(evidence.getSnapshotId());
        intent.setLastSnapshotCheckAt(now);
        if (Boolean.TRUE.equals(evidence.getSatisfied())) {
            intent.setSnapshotResult(JobControlIntentContract.SNAPSHOT_CONFIRMED);
            intent.setWaitingReason(null);
            clearSourceEvidence(intent);
            return toResult(jobControlIntentRepository.save(intent));
        }
        intent.setWaitingReason(evidence.getWaitingReason());
        if (intent.getConfirmationDeadline() != null
                && !now.isBefore(intent.getConfirmationDeadline())) {
            SnapshotSourceHealthService.SourceHealthDecision sourceDecision =
                    snapshotSourceHealthService.evaluateTimeoutEvidence(
                            intent.getTableAssetKey(),
                            intent.getConfirmationDeadline());
            intent.setSourceHealth(sourceDecision.sourceHealth());
            intent.setSourceHealthDetail(sourceDecision.detail());
            intent.setSourceEvidenceCheckedAt(sourceDecision.evidenceCheckedAt());
            if (sourceDecision.timeoutConclusionAllowed()) {
                intent.setSnapshotResult(JobControlIntentContract.SNAPSHOT_NOT_ADVANCED);
            } else {
                intent.setWaitingReason("SOURCE_BLOCKED [" + sourceDecision.sourceHealth() + "]: "
                        + sourceDecision.detail() + "; snapshot=" + evidence.getWaitingReason());
            }
        }
        return toResult(jobControlIntentRepository.save(intent));
    }

    /**
     * Check every control intent still awaiting attributable writer output.
     *
     * @return current result for each waiting control intent
     */
    public List<JobControlConfirmationResult> checkWaitingIntents() {
        return jobControlIntentRepository
                .findBySnapshotResultOrderByCreatedAtAsc(JobControlIntentContract.WAITING)
                .stream()
                .map(intent -> checkIntent(intent.getId()))
                .toList();
    }

    /** Load one control intent or fail with a scheduler-domain validation error. */
    private JobControlIntent getIntent(Long intentId) {
        if (intentId == null || intentId <= 0) {
            throw new IllegalArgumentException("job control intent id must be positive");
        }
        return jobControlIntentRepository.findByIdForUpdate(intentId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Job control intent not found: " + intentId));
    }

    /** Remove stale source evidence after a positive writer snapshot match. */
    private void clearSourceEvidence(JobControlIntent intent) {
        intent.setSourceHealth(null);
        intent.setSourceHealthDetail(null);
        intent.setSourceEvidenceCheckedAt(null);
    }

    /** Convert persisted job-control evidence into a stable result view. */
    private JobControlConfirmationResult toResult(JobControlIntent intent) {
        return new JobControlConfirmationResult(
                intent.getId(),
                intent.getIntentKey(),
                intent.getWriterJobKey(),
                intent.getWriterEpoch(),
                intent.getSnapshotResult(),
                intent.getBaselineSnapshotId(),
                intent.getObservedSnapshotId(),
                intent.getWaitingReason(),
                intent.getSourceHealth(),
                intent.getSourceHealthDetail(),
                intent.getSourceEvidenceCheckedAt(),
                intent.getLastSnapshotCheckAt());
    }

    /**
     * Snapshot-only result for one platform writer generation.
     *
     * @param intentId control intent id
     * @param intentKey epoch-derived intent key
     * @param writerJobKey stable writer key
     * @param writerEpoch fenced writer generation
     * @param snapshotResult independent snapshot outcome
     * @param baselineSnapshotId pre-operation snapshot
     * @param observedSnapshotId latest relevant snapshot
     * @param waitingReason current wait reason
     * @param sourceHealth independent source-health outcome
     * @param sourceHealthDetail source reconciliation detail
     * @param sourceEvidenceCheckedAt source evidence timestamp
     * @param lastSnapshotCheckAt latest evaluation timestamp
     */
    public record JobControlConfirmationResult(
            Long intentId,
            String intentKey,
            String writerJobKey,
            Long writerEpoch,
            String snapshotResult,
            String baselineSnapshotId,
            String observedSnapshotId,
            String waitingReason,
            String sourceHealth,
            String sourceHealthDetail,
            LocalDateTime sourceEvidenceCheckedAt,
            LocalDateTime lastSnapshotCheckAt) {
    }
}
