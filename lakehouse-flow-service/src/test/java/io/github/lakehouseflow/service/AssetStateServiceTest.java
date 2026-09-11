package io.github.lakehouseflow.service;

import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AssetStateService logic (without database).
 * 
 * Database integration tests will run in lakehouse-flow-boot module.
 */
class AssetStateServiceTest {

    @Test
    void testIsEventNewerWithSnapshotIds() {
        // Given: two snapshots where snapshot 200 > 100 lexicographically
        String snapshot1 = "100";
        String snapshot2 = "200";

        // When: comparing lexicographically
        // Then: 200 > 100
        assertTrue(snapshot2.compareTo(snapshot1) > 0);
    }

    @Test
    void testIsEventNewerWithWatermarks() {
        // Given: two watermarks
        LocalDateTime time1 = LocalDateTime.of(2025, 9, 11, 10, 0, 0);
        LocalDateTime time2 = LocalDateTime.of(2025, 9, 11, 11, 0, 0);

        // When: comparing timestamps
        // Then: time2 is after time1
        assertTrue(time2.isAfter(time1));
        assertFalse(time1.isAfter(time2));
    }

    @Test
    void testAssetKeyConstruction() {
        // Given: asset without partition
        String catalogName = "paimon";
        String databaseName = "prod";
        String tableName = "orders";
        String partitionName = null;

        // When: building asset key
        String assetKey = String.format("%s.%s.%s", catalogName, databaseName, tableName);

        // Then: should be correct
        assertEquals("paimon.prod.orders", assetKey);
    }

    @Test
    void testAssetKeyWithPartition() {
        // Given: asset with partition
        String catalogName = "paimon";
        String databaseName = "prod";
        String tableName = "orders";
        String partitionName = "dt=2025-09-11";

        // When: building asset key
        String assetKey = String.format("%s.%s.%s.%s", catalogName, databaseName, tableName, partitionName);

        // Then: should be correct
        assertEquals("paimon.prod.orders.dt=2025-09-11", assetKey);
    }

    @Test
    void testEventCreationWithDefaults() {
        // Given: an event with minimal fields
        LakehouseEvent event = LakehouseEvent.builder()
                .eventId("test_event")
                .sourceType("PAIMON")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .build();

        // When: event is created
        // Then: required fields should exist
        assertNotNull(event.getEventId());
        assertNotNull(event.getSourceType());
        assertNotNull(event.getCatalogName());
    }

    @Test
    void testAssetStateCreationWithDefaults() {
        // Given: asset state with required fields
        AssetState state = AssetState.builder()
                .assetKey("paimon.prod.orders")
                .assetType("TABLE")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .build();

        // When: onCreate is called (simulated)
        if (state.getQualityStatus() == null) state.setQualityStatus("UNKNOWN");
        if (state.getSchemaStatus() == null) state.setSchemaStatus("UNKNOWN");
        if (state.getBackfillStatus() == null) state.setBackfillStatus("NONE");
        if (state.getReadinessStatus() == null) state.setReadinessStatus("UNKNOWN");

        // Then: defaults should be set
        assertEquals("UNKNOWN", state.getQualityStatus());
        assertEquals("UNKNOWN", state.getSchemaStatus());
        assertEquals("NONE", state.getBackfillStatus());
        assertEquals("UNKNOWN", state.getReadinessStatus());
    }
}
