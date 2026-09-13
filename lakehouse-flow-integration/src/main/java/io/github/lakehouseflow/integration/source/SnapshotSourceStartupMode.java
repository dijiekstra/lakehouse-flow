package io.github.lakehouseflow.integration.source;

/**
 * Initial history position used when a snapshot source has no durable offset yet.
 */
public enum SnapshotSourceStartupMode {

    /** Read only the latest retained snapshot and establish the current scheduling fact. */
    LATEST,

    /** Replay retained history from the earliest available snapshot by explicit opt-in. */
    EARLIEST
}
