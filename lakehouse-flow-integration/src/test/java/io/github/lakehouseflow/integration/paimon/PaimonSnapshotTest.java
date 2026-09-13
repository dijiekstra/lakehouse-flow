package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests conversion of native Paimon metadata to the common snapshot contract.
 */
class PaimonSnapshotTest {

    /**
     * Verify snapshot properties and changed partitions survive conversion as attribution evidence.
     */
    @Test
    void toLakehouseSnapshotPreservesPaimonEvidence() {
        long commitMillis = Instant.parse("2026-09-13T04:00:00Z").toEpochMilli();
        PaimonSnapshot paimon = new PaimonSnapshot(
                42L,
                7L,
                "writer-1",
                99L,
                "APPEND",
                commitMillis,
                commitMillis,
                1_000L,
                50L,
                20L,
                Map.of("lakehouse-flow.intent-key", "task-instance:12"),
                List.of("dt=2026-09-13"));

        LakehouseSnapshot snapshot = paimon.toLakehouseSnapshot(ZoneId.of("Asia/Shanghai"));

        assertEquals("42", snapshot.sourceOffset());
        assertEquals("42", snapshot.snapshotId());
        assertEquals("7", snapshot.schemaId());
        assertEquals(12, snapshot.commitTime().getHour());
        assertEquals(Map.of("lakehouse-flow.intent-key", "task-instance:12"),
                snapshot.snapshotProperties());
        assertEquals(List.of("dt=2026-09-13"), snapshot.changedPartitions());
        assertEquals("42", snapshot.payload().get("nativeSnapshotId"));
    }

    /**
     * Verify Paimon's no-watermark sentinel is represented as missing evidence.
     */
    @Test
    void toLakehouseSnapshotTreatsMinimumWatermarkAsAbsent() {
        PaimonSnapshot paimon = new PaimonSnapshot(
                1L, 1L, "writer", 1L, "APPEND", 0L, Long.MIN_VALUE,
                null, null, null, null, null);

        LakehouseSnapshot snapshot = paimon.toLakehouseSnapshot(ZoneId.of("UTC"));

        assertNull(snapshot.watermark());
        assertEquals(Map.of(), snapshot.snapshotProperties());
        assertEquals(List.of(), snapshot.changedPartitions());
    }

    /**
     * Verify Paimon maintenance snapshots cannot masquerade as business-data progress.
     */
    @Test
    void toLakehouseSnapshotClassifiesCompactionAsMaintenance() {
        PaimonSnapshot paimon = new PaimonSnapshot(
                2L, 1L, "writer", 2L, "COMPACT", 1L, null,
                null, null, null, Map.of(), List.of("dt=2026-09-13"));

        LakehouseSnapshot snapshot = paimon.toLakehouseSnapshot(ZoneId.of("UTC"));

        assertEquals(false, snapshot.dataChange());
        assertEquals("COMPACT", snapshot.commitKind());
    }
}
