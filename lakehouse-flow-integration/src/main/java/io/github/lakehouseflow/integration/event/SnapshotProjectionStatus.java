package io.github.lakehouseflow.integration.event;

/**
 * Consistency of the durable event and table-level AssetState projection for one source.
 */
public enum SnapshotProjectionStatus {

    /** No projection is expected because the source has no durable offset. */
    NOT_APPLICABLE,

    /** Latest durable event and table-level AssetState contain the expected snapshot. */
    CONSISTENT,

    /** A durable offset exists but its latest durable event cannot be found. */
    MISSING_EVENT,

    /** A durable event exists but its table-level AssetState is absent. */
    MISSING_ASSET_STATE,

    /** Durable event and table-level AssetState disagree on the latest snapshot. */
    DRIFT,

    /** A durable data event exists but the AssetState data coordinate is absent. */
    MISSING_DATA_STATE,

    /** Latest durable data event and AssetState data coordinate disagree. */
    DATA_DRIFT,

    /** AssetState claims data progress without a durable typed data event. */
    ORPHANED_DATA_PROJECTION,

    /** The event or AssetState exists even though no durable source offset exists. */
    ORPHANED_PROJECTION,

    /** The source is in sync but its latest snapshot is absent from durable events. */
    EVENT_OFFSET_MISMATCH,

    /** Projection evidence could not be read. */
    ERROR
}
