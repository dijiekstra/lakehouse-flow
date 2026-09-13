package io.github.lakehouseflow.integration.source;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Format-neutral observation of one committed lakehouse table snapshot.
 *
 * @param sourceOffset opaque source position persisted after successful projection
 * @param snapshotId adapter-normalized, monotonically ordered snapshot coordinate used by the
 *     scheduler; adapters retain a different native identifier in {@code payload} when necessary
 * @param schemaId source-native schema identifier associated with the snapshot; it is evidence,
 *     not a cross-format ordering coordinate
 * @param watermark business/event-time watermark when the format exposes one
 * @param commitKind format-specific commit kind normalized as text
 * @param dataChange whether the adapter classified this snapshot as a business-data change
 * @param commitTime snapshot commit time
 * @param snapshotProperties immutable format-native commit properties normalized as text
 * @param changedPartitions immutable changed-partition evidence in scheduler asset-key syntax
 * @param payload immutable additional source evidence retained for audit
 */
public record LakehouseSnapshot(
        String sourceOffset,
        String snapshotId,
        String schemaId,
        LocalDateTime watermark,
        String commitKind,
        boolean dataChange,
        LocalDateTime commitTime,
        Map<String, String> snapshotProperties,
        List<String> changedPartitions,
        Map<String, Object> payload) {

    /**
     * Validate required snapshot coordinates and defensively copy source evidence.
     */
    public LakehouseSnapshot {
        if (sourceOffset == null || sourceOffset.isBlank()) {
            throw new IllegalArgumentException("sourceOffset must not be blank");
        }
        if (snapshotId == null || snapshotId.isBlank()) {
            throw new IllegalArgumentException("snapshotId must not be blank");
        }
        snapshotProperties = snapshotProperties == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(snapshotProperties));
        changedPartitions = changedPartitions == null ? List.of() : List.copyOf(changedPartitions);
        payload = payload == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }
}
