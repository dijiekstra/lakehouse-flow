package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PaimonSnapshot Tests
 *
 * Tests:
 * 1. Convert PaimonSnapshot to LakehouseEvent
 * 2. Generate deterministic event ID
 * 3. Handle timestamp conversion correctly
 * 4. Handle watermark string conversion to LocalDateTime
 */
@DisplayName("Paimon Snapshot Tests")
class PaimonSnapshotTest {

    @Test
    @DisplayName("Should convert snapshot to LakehouseEvent")
    void testToLakehouseEvent() {
        // Arrange
        long currentTimeMillis = System.currentTimeMillis();
        PaimonSnapshot snapshot = PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc123")
                .commitKind("APPEND")
                .commitTime(currentTimeMillis)
                .watermark("2026-09-11T10:00:00")
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build();

        // Act
        LakehouseEvent event = snapshot.toLakehouseEvent("paimon_catalog", "ods", "orders");

        // Assert
        assertEquals("PAIMON:paimon_catalog:ods:orders:1000", event.getEventId());
        assertEquals("SNAPSHOT_COMMITTED", event.getEventType());
        assertEquals("PAIMON", event.getSourceType());
        assertEquals("paimon_catalog", event.getCatalogName());
        assertEquals("ods", event.getDatabaseName());
        assertEquals("orders", event.getTableName());
        assertEquals("1000", event.getSnapshotId());
        assertEquals("100", event.getSchemaId());
        assertEquals("APPEND", event.getCommitKind());
        assertNotNull(event.getCommitTime());
        assertNotNull(event.getWatermark());
    }

    @Test
    @DisplayName("Should generate deterministic event ID")
    void testDeterministicEventId() {
        // Arrange
        PaimonSnapshot snapshot1 = PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark("2026-09-11T10:00:00")
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build();

        PaimonSnapshot snapshot2 = PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("different_user")
                .commitIdentifier("different_commit")
                .commitKind("APPEND")
                .commitTime(123456789L)
                .watermark("2026-09-12T10:00:00")
                .deltaRecordCount(2000L)
                .changelogRecordCount(800L)
                .build();

        // Act
        String eventId1 = snapshot1.toLakehouseEvent("paimon_catalog", "ods", "orders").getEventId();
        String eventId2 = snapshot2.toLakehouseEvent("paimon_catalog", "ods", "orders").getEventId();

        // Assert
        // Same snapshot ID should produce same event ID regardless of other fields
        assertEquals(eventId1, eventId2);
        assertEquals("PAIMON:paimon_catalog:ods:orders:1000", eventId1);
    }

    @Test
    @DisplayName("Should handle null watermark")
    void testNullWatermark() {
        // Arrange
        PaimonSnapshot snapshot = PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark(null)  // No watermark
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build();

        // Act
        LakehouseEvent event = snapshot.toLakehouseEvent("paimon_catalog", "ods", "orders");

        // Assert
        assertNull(event.getWatermark());
        assertEquals("PAIMON:paimon_catalog:ods:orders:1000", event.getEventId());
    }

    @Test
    @DisplayName("Should handle invalid watermark format gracefully")
    void testInvalidWatermarkFormat() {
        // Arrange
        PaimonSnapshot snapshot = PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark("invalid-watermark-format")  // Invalid format
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build();

        // Act
        LakehouseEvent event = snapshot.toLakehouseEvent("paimon_catalog", "ods", "orders");

        // Assert
        // Should not throw, watermark should be null
        assertNull(event.getWatermark());
        assertEquals("PAIMON:paimon_catalog:ods:orders:1000", event.getEventId());
    }

    @Test
    @DisplayName("Should convert commitTime correctly")
    void testCommitTimeConversion() {
        // Arrange
        long currentTimeMillis = System.currentTimeMillis();
        PaimonSnapshot snapshot = PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc")
                .commitKind("APPEND")
                .commitTime(currentTimeMillis)
                .watermark("2026-09-11T10:00:00")
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build();

        // Act
        LakehouseEvent event = snapshot.toLakehouseEvent("paimon_catalog", "ods", "orders");

        // Assert
        assertNotNull(event.getCommitTime());
        assertTrue(event.getCommitTime() instanceof LocalDateTime);
    }

    @Test
    @DisplayName("Should include payload metadata")
    void testPayloadMetadata() {
        // Arrange
        PaimonSnapshot snapshot = PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc123")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark("2026-09-11T10:00:00")
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build();

        // Act
        LakehouseEvent event = snapshot.toLakehouseEvent("paimon_catalog", "ods", "orders");

        // Assert
        assertNotNull(event.getPayloadJson());
        assertTrue(event.getPayloadJson().containsKey("snapshotId"));
        assertTrue(event.getPayloadJson().containsKey("schemaId"));
        assertTrue(event.getPayloadJson().containsKey("commitUser"));
        assertTrue(event.getPayloadJson().containsKey("commitIdentifier"));
        assertTrue(event.getPayloadJson().containsKey("commitKind"));
        assertTrue(event.getPayloadJson().containsKey("watermark"));
        assertTrue(event.getPayloadJson().containsKey("deltaRecordCount"));
        assertTrue(event.getPayloadJson().containsKey("changelogRecordCount"));

        assertEquals("1000", event.getPayloadJson().get("snapshotId"));
        assertEquals("airflow", event.getPayloadJson().get("commitUser"));
    }
}
