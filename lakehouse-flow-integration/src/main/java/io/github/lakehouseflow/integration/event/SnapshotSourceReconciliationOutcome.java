package io.github.lakehouseflow.integration.event;

/**
 * Operational outcome of one snapshot source reconciliation pass.
 */
public enum SnapshotSourceReconciliationOutcome {

    /** Source, durable offset, event, and AssetState evidence are consistent. */
    HEALTHY,

    /** A bounded metadata rescan or event projection replay can repair the difference. */
    REPAIRABLE,

    /** Automatic repair would risk skipping or inventing snapshot evidence. */
    BLOCKED
}
