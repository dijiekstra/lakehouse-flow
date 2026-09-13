package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;

import java.time.LocalDateTime;

/**
 * Auditable source-to-projection reconciliation result.
 *
 * @param identity configured source and managed table identity
 * @param outcome operational reconciliation outcome
 * @param offsetStatus relationship between source metadata and durable offset
 * @param projectionStatus relationship between durable event and AssetState
 * @param durableOffset last completely projected source offset, or null
 * @param earliestRetainedOffset earliest currently retained source offset, or null
 * @param latestSourceOffset latest currently available source offset, or null
 * @param latestSourceSnapshotId normalized latest source snapshot coordinate, or null
 * @param latestEventSnapshotId latest durable event snapshot coordinate, or null
 * @param assetStateSnapshotId table-level AssetState snapshot coordinate, or null
 * @param latestDataEventSnapshotId latest durable business-data event coordinate, or null
 * @param assetStateDataSnapshotId table-level AssetState business-data coordinate, or null
 * @param pendingOffsetCount source offsets awaiting ingestion, or null when unknown
 * @param repairAttempted whether this pass attempted bounded compensation
 * @param repairedEventCount number of newly inserted events during compensation
 * @param detail concise reconciliation evidence
 * @param checkedAt reconciliation timestamp
 */
public record SnapshotSourceReconciliation(
        LakehouseSourceIdentity identity,
        SnapshotSourceReconciliationOutcome outcome,
        SnapshotSourceOffsetStatus offsetStatus,
        SnapshotProjectionStatus projectionStatus,
        String durableOffset,
        String earliestRetainedOffset,
        String latestSourceOffset,
        String latestSourceSnapshotId,
        String latestEventSnapshotId,
        String assetStateSnapshotId,
        String latestDataEventSnapshotId,
        String assetStateDataSnapshotId,
        Long pendingOffsetCount,
        boolean repairAttempted,
        int repairedEventCount,
        String detail,
        LocalDateTime checkedAt) {

    /** Validate required reconciliation identity and status fields. */
    public SnapshotSourceReconciliation {
        if (identity == null || outcome == null || offsetStatus == null || projectionStatus == null) {
            throw new IllegalArgumentException("reconciliation identity and statuses must not be null");
        }
        if (repairedEventCount < 0) {
            throw new IllegalArgumentException("repairedEventCount must not be negative");
        }
        if (checkedAt == null) {
            throw new IllegalArgumentException("checkedAt must not be null");
        }
    }
}
