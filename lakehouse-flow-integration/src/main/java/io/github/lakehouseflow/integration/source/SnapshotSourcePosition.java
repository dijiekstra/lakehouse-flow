package io.github.lakehouseflow.integration.source;

/**
 * Format-neutral source position returned by a lakehouse adapter during reconciliation.
 *
 * @param status relationship between the durable offset and current source range
 * @param durableOffset offset currently committed by Lakehouse Flow, or null
 * @param earliestRetainedOffset earliest source offset still available, or null for an empty source
 * @param latestSourceOffset latest source offset currently available, or null for an empty source
 * @param latestSnapshotId normalized snapshot coordinate at the latest source offset, or null
 * @param pendingOffsetCount number of source offsets still awaiting ingestion, or null when unknown
 * @param detail human-readable reconciliation evidence
 */
public record SnapshotSourcePosition(
        SnapshotSourceOffsetStatus status,
        String durableOffset,
        String earliestRetainedOffset,
        String latestSourceOffset,
        String latestSnapshotId,
        Long pendingOffsetCount,
        String detail) {

    /** Validate required status and non-negative lag evidence. */
    public SnapshotSourcePosition {
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (pendingOffsetCount != null && pendingOffsetCount < 0) {
            throw new IllegalArgumentException("pendingOffsetCount must not be negative");
        }
    }
}
