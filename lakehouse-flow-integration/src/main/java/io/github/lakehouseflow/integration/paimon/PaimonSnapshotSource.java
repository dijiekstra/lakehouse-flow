package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.model.LakehouseEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Paimon Snapshot Source - Adapter for scanning Paimon $snapshots table.
 *
 * This is a mock implementation for Phase 2. In production:
 * - Connect to actual Paimon catalog via JDBC
 * - Execute: SELECT * FROM catalog.db.`table$snapshots` WHERE snapshot_id > ?
 * - Map ResultSet to PaimonSnapshot
 *
 * For now, this mock returns sample data for testing the event ingestion loop.
 */
@Component
@Slf4j
public class PaimonSnapshotSource {

    private String catalogName = "paimon_catalog";
    private String databaseName = "ods";
    private String tableName = "orders";

    /**
     * Scan Paimon snapshots since a given snapshot ID.
     *
     * @param sinceSnapshotId Last processed snapshot ID (exclusive)
     * @return List of new PaimonSnapshot objects
     *
     * Mock implementation: returns sample snapshots for testing.
     * Production: would query actual Paimon $snapshots table via JDBC.
     */
    public List<PaimonSnapshot> scanSnapshots(Long sinceSnapshotId) {
        List<PaimonSnapshot> snapshots = new ArrayList<>();

        // Mock data for testing event ingestion
        // In production, this would be actual Paimon snapshots from JDBC query
        if (sinceSnapshotId == null || sinceSnapshotId < 1000) {
            snapshots.add(PaimonSnapshot.builder()
                    .snapshotId("1000")
                    .schemaId("100")
                    .commitUser("airflow")
                    .commitIdentifier("commit_abc123")
                    .commitKind("APPEND")
                    .commitTime(System.currentTimeMillis())
                    .watermark("2026-09-11T10:00:00")
                    .deltaRecordCount(1000L)
                    .changelogRecordCount(500L)
                    .build());
        }

        if (sinceSnapshotId == null || sinceSnapshotId < 1001) {
            snapshots.add(PaimonSnapshot.builder()
                    .snapshotId("1001")
                    .schemaId("100")
                    .commitUser("airflow")
                    .commitIdentifier("commit_def456")
                    .commitKind("APPEND")
                    .commitTime(System.currentTimeMillis())
                    .watermark("2026-09-11T12:00:00")
                    .deltaRecordCount(2000L)
                    .changelogRecordCount(800L)
                    .build());
        }

        log.debug("Scanned {} snapshots since {}", snapshots.size(), sinceSnapshotId);
        return snapshots;
    }

    /**
     * Convert PaimonSnapshot to LakehouseEvent.
     *
     * @param snapshot Paimon snapshot to convert
     * @return LakehouseEvent ready to persist
     */
    public LakehouseEvent mapToLakehouseEvent(PaimonSnapshot snapshot) {
        LakehouseEvent event = snapshot.toLakehouseEvent(catalogName, databaseName, tableName);
        log.debug("Mapped snapshot {} to event {}", snapshot.getSnapshotId(), event.getEventId());
        return event;
    }

    /**
     * Get the catalog name for this source.
     */
    public String getCatalogName() {
        return catalogName;
    }

    /**
     * Get the database name for this source.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Get the table name for this source.
     */
    public String getTableName() {
        return tableName;
    }
}
