package io.github.lakehouseflow.integration.source;

/**
 * Relationship between a durable ingestion offset and the current lakehouse source range.
 */
public enum SnapshotSourceOffsetStatus {

    /** The source has no retained snapshots and Lakehouse Flow has no durable offset. */
    EMPTY,

    /** The source has snapshots but Lakehouse Flow has not established its first offset. */
    UNINITIALIZED,

    /** The durable offset equals the latest source offset observed during reconciliation. */
    IN_SYNC,

    /** New retained source snapshots exist after the durable offset. */
    LAGGING,

    /** Required snapshots between the durable offset and the retained range have expired. */
    RETENTION_GAP,

    /** The durable offset is newer than the source range or the source unexpectedly became empty. */
    OFFSET_AHEAD,

    /** Source metadata could not be inspected. */
    ERROR
}
