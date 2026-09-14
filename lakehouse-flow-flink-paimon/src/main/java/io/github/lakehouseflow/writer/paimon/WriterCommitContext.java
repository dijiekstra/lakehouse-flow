package io.github.lakehouseflow.writer.paimon;

import io.github.lakehouseflow.common.AssetKeys;
import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.common.SnapshotEvidenceContract;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable writer identity and snapshot attribution carried into one Paimon commit.
 *
 * @param tableAssetKey normalized catalog.database.table owned by the writer
 * @param writerJobKey stable platform writer key
 * @param writerEpoch currently authorized writer generation
 * @param intentKey scheduling or job-control intent authorizing the commit
 * @param intentKind kind of immutable Lakehouse Flow instruction
 * @param processingMode engine-neutral STREAMING or BATCH mode
 * @param snapshotProperties exact properties to persist in the Paimon snapshot
 */
public record WriterCommitContext(
        String tableAssetKey,
        String writerJobKey,
        long writerEpoch,
        String intentKey,
        IntentKind intentKind,
        String processingMode,
        Map<String, String> snapshotProperties) implements Serializable {

    /** Validate and normalize every fencing and attribution coordinate. */
    public WriterCommitContext {
        tableAssetKey = requireTableKey(tableAssetKey);
        writerJobKey = requireText(writerJobKey, "writerJobKey");
        if (writerEpoch <= 0) {
            throw new IllegalArgumentException("writerEpoch must be positive");
        }
        intentKey = requireText(intentKey, "intentKey");
        intentKind = Objects.requireNonNull(intentKind, "intentKind must not be null");
        processingMode = ScheduleNodeProcessingModes.normalize(processingMode);
        snapshotProperties = snapshotProperties == null ? Map.of() : Map.copyOf(snapshotProperties);
        validateProperties(writerJobKey, writerEpoch, intentKey, intentKind, tableAssetKey, snapshotProperties);
    }

    /** Return true when this commit proves a platform writer lifecycle instruction. */
    public boolean jobControlIntent() {
        return IntentKind.JOB_CONTROL == intentKind;
    }

    /** Validate common writer identity and kind-specific correlation properties. */
    private static void validateProperties(
            String writerJobKey,
            long writerEpoch,
            String intentKey,
            IntentKind intentKind,
            String tableAssetKey,
            Map<String, String> properties) {
        requireProperty(properties, SnapshotEvidenceContract.SOURCE_PROPERTY,
                SnapshotEvidenceContract.INTENT_SOURCE);
        requireProperty(properties, SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY, writerJobKey);
        requireProperty(properties, SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY,
                Long.toString(writerEpoch));
        if (IntentKind.JOB_CONTROL == intentKind) {
            requireProperty(properties, SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY, intentKey);
            rejectProperty(properties, SnapshotEvidenceContract.INTENT_KEY_PROPERTY);
            return;
        }
        requireProperty(properties, SnapshotEvidenceContract.INTENT_KEY_PROPERTY, intentKey);
        rejectProperty(properties, SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY);
        String targetAsset = requireText(
                properties.get(SnapshotEvidenceContract.TARGET_ASSET_PROPERTY),
                SnapshotEvidenceContract.TARGET_ASSET_PROPERTY);
        if (!tableAssetKey.equals(requireTableKey(targetAsset))) {
            throw new IllegalArgumentException("Snapshot target asset does not belong to writer table: "
                    + targetAsset);
        }
        requireProperty(properties, SnapshotEvidenceContract.FINAL_PROPERTY,
                SnapshotEvidenceContract.FINAL_VALUE);
    }

    /** Require one exact snapshot property value. */
    private static void requireProperty(Map<String, String> properties, String key, String expected) {
        String actual = properties.get(key);
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(
                    "Snapshot property " + key + " must equal " + expected + " but was " + actual);
        }
    }

    /** Reject correlation properties from the other intent kind. */
    private static void rejectProperty(Map<String, String> properties, String key) {
        if (properties.containsKey(key)) {
            throw new IllegalArgumentException("Snapshot property is not allowed for this intent kind: " + key);
        }
    }

    /** Normalize a table or partition key to catalog.database.table. */
    private static String requireTableKey(String assetKey) {
        return AssetKeys.tableKey(assetKey)
                .orElseThrow(() -> new IllegalArgumentException(
                        "tableAssetKey must use catalog.database.table[.partition]"));
    }

    /** Require one nonblank textual value. */
    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /** Immutable instruction kinds understood by the writer adapter. */
    public enum IntentKind {
        /** Platform lifecycle instruction for a continuous writer. */
        JOB_CONTROL,

        /** Flow scheduling instruction for one logical data-processing range. */
        DATA_PROCESSING
    }
}
