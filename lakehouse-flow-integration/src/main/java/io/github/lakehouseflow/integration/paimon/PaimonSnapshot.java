package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.model.LakehouseEvent;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * Paimon Snapshot - Represents a snapshot from Paimon $snapshots table.
 *
 * Used as intermediate DTO when scanning Paimon snapshots,
 * before converting to LakehouseEvent.
 */
@Getter
@Setter
@Builder
public class PaimonSnapshot {

    /**
     * Snapshot ID (unique within a table).
     */
    private String snapshotId;

    /**
     * Schema ID associated with this snapshot.
     */
    private String schemaId;

    /**
     * User who committed this snapshot.
     */
    private String commitUser;

    /**
     * Commit identifier (git-like commit identifier).
     */
    private String commitIdentifier;

    /**
     * Commit kind: APPEND, OVERWRITE, CHERRY_PICK, etc.
     */
    private String commitKind;

    /**
     * Timestamp when commit happened.
     */
    private Long commitTime;

    /**
     * Watermark (event time) if available.
     */
    private String watermark;

    /**
     * Number of delta records in this snapshot.
     */
    private Long deltaRecordCount;

    /**
     * Number of changelog records in this snapshot.
     */
    private Long changelogRecordCount;

    /**
     * Convert PaimonSnapshot to LakehouseEvent.
     *
     * @param catalogName Paimon catalog name
     * @param databaseName Database name
     * @param tableName Table name
     * @return LakehouseEvent ready to be persisted
     */
    public LakehouseEvent toLakehouseEvent(String catalogName, String databaseName, String tableName) {
        String eventId = generateEventId(catalogName, databaseName, tableName);

        // Convert commitTime from Long (milliseconds) to LocalDateTime
        LocalDateTime commitDateTime = null;
        if (commitTime != null) {
            commitDateTime = java.time.Instant
                    .ofEpochMilli(commitTime)
                    .atZone(java.time.ZoneId.systemDefault())
                    .toLocalDateTime();
        }

        // Convert watermark from ISO string to LocalDateTime
        LocalDateTime watermarkDateTime = null;
        if (watermark != null && !watermark.isEmpty()) {
            try {
                watermarkDateTime = LocalDateTime.parse(watermark,
                        java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (Exception e) {
                // If watermark format is invalid, log and leave as null
                System.err.println("Invalid watermark format: " + watermark);
            }
        }

        return LakehouseEvent.builder()
                .eventId(eventId)
                .eventType("SNAPSHOT_COMMITTED")
                .sourceType("PAIMON")
                .catalogName(catalogName)
                .databaseName(databaseName)
                .tableName(tableName)
                .snapshotId(snapshotId)
                .schemaId(schemaId)
                .watermark(watermarkDateTime)
                .commitKind(commitKind)
                .commitTime(commitDateTime)
                .payloadJson(toPayloadMap())
                .build();
    }

    /**
     * Generate deterministic event ID.
     * Format: PAIMON:catalogName:databaseName:tableName:snapshotId
     */
    private String generateEventId(String catalogName, String databaseName, String tableName) {
        return String.format("PAIMON:%s:%s:%s:%s",
                catalogName, databaseName, tableName, snapshotId);
    }

    /**
     * Convert snapshot to payload map.
     */
    private java.util.Map<String, Object> toPayloadMap() {
        java.util.Map<String, Object> payload = new java.util.HashMap<>();
        payload.put("snapshotId", snapshotId);
        payload.put("schemaId", schemaId);
        payload.put("commitUser", commitUser);
        payload.put("commitIdentifier", commitIdentifier);
        payload.put("commitKind", commitKind);
        payload.put("commitTime", commitTime);
        payload.put("watermark", watermark);
        payload.put("deltaRecordCount", deltaRecordCount);
        payload.put("changelogRecordCount", changelogRecordCount);
        return payload;
    }
}
