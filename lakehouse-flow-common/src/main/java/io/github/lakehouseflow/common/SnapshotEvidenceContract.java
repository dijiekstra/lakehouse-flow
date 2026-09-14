package io.github.lakehouseflow.common;

/**
 * Shared scheduling-intent and snapshot-evidence contract constants.
 *
 * Downstream integrations copy the required property keys and values from the
 * scheduling intent into the final target snapshot. Lakehouse Flow then uses
 * those immutable properties for attribution without reading runtime status.
 */
public final class SnapshotEvidenceContract {

    /** Current version of the scheduling-intent payload contract. */
    public static final String CONTRACT_VERSION = "1.3";

    /** Producer identity written into every scheduling-intent payload. */
    public static final String INTENT_SOURCE = "LAKEHOUSE_FLOW";

    /** Confirmation mode requiring an exact scheduling-intent marker. */
    public static final String CONFIRMATION_MODE = "INTENT_CORRELATED";

    /** Snapshot property identifying the system that issued the instruction. */
    public static final String SOURCE_PROPERTY = "lakehouse-flow.source";

    /** Snapshot property carrying the immutable scheduling intent key. */
    public static final String INTENT_KEY_PROPERTY = "lakehouse-flow.intent-key";

    /** Snapshot property carrying the immutable job-control intent key. */
    public static final String JOB_CONTROL_INTENT_KEY_PROPERTY = "lakehouse-flow.job-control-intent-key";

    /** Snapshot property identifying the unique writer job for the physical table. */
    public static final String WRITER_JOB_KEY_PROPERTY = "lakehouse-flow.writer-job-key";

    /** Snapshot property carrying the fenced writer generation. */
    public static final String WRITER_EPOCH_PROPERTY = "lakehouse-flow.writer-epoch";

    /** Snapshot property carrying the managed target asset key. */
    public static final String TARGET_ASSET_PROPERTY = "lakehouse-flow.target-asset";

    /** Snapshot property carrying the business date of the instruction. */
    public static final String BIZ_DATE_PROPERTY = "lakehouse-flow.biz-date";

    /** Snapshot property indicating that the data-producing commit is final. */
    public static final String FINAL_PROPERTY = "lakehouse-flow.final";

    /** Required value of the final snapshot property. */
    public static final String FINAL_VALUE = "true";

    /** Format-neutral raw-event field indicating that a snapshot contains business-data changes. */
    public static final String DATA_CHANGE_FIELD = "dataChange";

    /** Format-neutral change type required by snapshot confirmation. */
    public static final String REQUIRED_CHANGE_TYPE = "DATA";

    /** Prevent construction of this constants-only class. */
    private SnapshotEvidenceContract() {
    }
}
