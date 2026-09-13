package io.github.lakehouseflow.integration.source;

import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests format-neutral event mapping and deterministic event identity.
 */
class LakehouseSnapshotEventMapperTest {

    /**
     * Verify a non-Paimon source maps without losing source evidence.
     */
    @Test
    void mapPreservesGenericSnapshotCoordinates() {
        LakehouseSourceIdentity identity = new LakehouseSourceIdentity(
                "hudi", "prod.sales.orders", "prod", "sales", "orders");
        LocalDateTime commitTime = LocalDateTime.of(2026, 9, 13, 12, 0);
        LakehouseSnapshot snapshot = new LakehouseSnapshot(
                "20260913120000",
                "20260913120000",
                "schema-v2",
                commitTime,
                "DELTA_COMMIT",
                true,
                commitTime,
                Map.of("lakehouse-flow.intent-key", "task-instance:42"),
                java.util.List.of("dt=2026-09-13"),
                Map.of("nativeSnapshotId", "20260913120000"));

        LakehouseEvent event = new LakehouseSnapshotEventMapper().map(identity, snapshot);

        assertEquals("HUDI:prod:sales:orders:20260913120000", event.getEventId());
        assertEquals("HUDI", event.getSourceType());
        assertEquals("schema-v2", event.getSchemaId());
        assertEquals(true, event.getDataChange());
        assertEquals(true, event.getPayloadJson().get("dataChange"));
        assertEquals(snapshot.snapshotProperties(), event.getPayloadJson().get("snapshotProperties"));
        assertEquals(snapshot.changedPartitions(), event.getPayloadJson().get("changedPartitions"));
        assertEquals("20260913120000", event.getPayloadJson().get("nativeSnapshotId"));
    }
}
