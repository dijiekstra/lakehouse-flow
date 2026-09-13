package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSnapshot;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Paimon-specific snapshot metadata before conversion to the common source model.
 *
 * @param snapshotId Paimon snapshot id
 * @param schemaId Paimon schema id
 * @param commitUser Paimon commit user
 * @param commitIdentifier writer transaction or checkpoint identifier
 * @param commitKind Paimon commit kind
 * @param commitTimeMillis commit epoch milliseconds
 * @param watermarkMillis event-time watermark, or {@link Long#MIN_VALUE} when absent
 * @param totalRecordCount total physical record count reported by Paimon
 * @param deltaRecordCount physical record-count delta
 * @param changelogRecordCount produced changelog record count
 * @param snapshotProperties immutable commit properties used for intent attribution
 * @param changedPartitions partitions referenced by the snapshot delta manifests
 */
public record PaimonSnapshot(
        long snapshotId,
        long schemaId,
        String commitUser,
        long commitIdentifier,
        String commitKind,
        long commitTimeMillis,
        Long watermarkMillis,
        Long totalRecordCount,
        Long deltaRecordCount,
        Long changelogRecordCount,
        Map<String, String> snapshotProperties,
        List<String> changedPartitions) {

    /**
     * Defensively copy Paimon collections because they become durable event evidence.
     */
    public PaimonSnapshot {
        snapshotProperties = snapshotProperties == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(snapshotProperties));
        changedPartitions = changedPartitions == null ? List.of() : List.copyOf(changedPartitions);
    }

    /**
     * Convert Paimon metadata to the format-neutral snapshot contract.
     *
     * @param zoneId local event-model time zone
     * @return common snapshot observation
     */
    public LakehouseSnapshot toLakehouseSnapshot(ZoneId zoneId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("snapshotId", Long.toString(snapshotId));
        payload.put("nativeSnapshotId", Long.toString(snapshotId));
        payload.put("schemaId", Long.toString(schemaId));
        payload.put("nativeSchemaId", Long.toString(schemaId));
        payload.put("commitUser", commitUser);
        payload.put("commitIdentifier", commitIdentifier);
        payload.put("commitKind", commitKind);
        payload.put("commitTime", commitTimeMillis);
        payload.put("watermark", watermarkMillis);
        payload.put("totalRecordCount", totalRecordCount);
        payload.put("deltaRecordCount", deltaRecordCount);
        payload.put("changelogRecordCount", changelogRecordCount);
        LocalDateTime commitTime = toLocalDateTime(commitTimeMillis, zoneId);
        LocalDateTime watermark = watermarkMillis == null || watermarkMillis == Long.MIN_VALUE
                ? null
                : toLocalDateTime(watermarkMillis, zoneId);
        return new LakehouseSnapshot(
                Long.toString(snapshotId),
                Long.toString(snapshotId),
                Long.toString(schemaId),
                watermark,
                commitKind,
                "APPEND".equalsIgnoreCase(commitKind) || "OVERWRITE".equalsIgnoreCase(commitKind),
                commitTime,
                snapshotProperties,
                changedPartitions,
                payload);
    }

    /**
     * Convert one epoch-millisecond value to the scheduler's local event representation.
     *
     * @param epochMillis source epoch milliseconds
     * @param zoneId configured source time zone
     * @return local date-time value
     */
    private static LocalDateTime toLocalDateTime(long epochMillis, ZoneId zoneId) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zoneId);
    }
}
