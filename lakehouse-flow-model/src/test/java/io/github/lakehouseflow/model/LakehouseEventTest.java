package io.github.lakehouseflow.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LakehouseEventTest {

    @Test
    void testLakehouseEventCreation() {
        LocalDateTime now = LocalDateTime.now();
        Map<String, Object> payload = new HashMap<>();
        payload.put("key", "value");

        LakehouseEvent event = LakehouseEvent.builder()
                .eventId("paimon_prod_orders_snapshot_123")
                .eventType("SNAPSHOT_COMPLETED")
                .sourceType("PAIMON")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .partitionName("dt=2025-09-11")
                .snapshotId("123")
                .schemaId("schema_v1")
                .watermark(now)
                .commitKind("APPEND")
                .commitTime(now)
                .payloadJson(payload)
                .observedAt(now)
                .createdAt(now)
                .updatedAt(now)
                .build();

        assertNotNull(event);
        assertEquals("paimon_prod_orders_snapshot_123", event.getEventId());
        assertEquals("SNAPSHOT_COMPLETED", event.getEventType());
        assertEquals("PAIMON", event.getSourceType());
        assertEquals("paimon.prod.orders", 
                event.getCatalogName() + "." + event.getDatabaseName() + "." + event.getTableName());
    }

    @Test
    void testEventIdUniqueness() {
        // Two events with same event_id should be treated as duplicates
        LakehouseEvent event1 = LakehouseEvent.builder()
                .eventId("same_id")
                .sourceType("PAIMON")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .observedAt(LocalDateTime.now())
                .build();

        LakehouseEvent event2 = LakehouseEvent.builder()
                .eventId("same_id")
                .sourceType("PAIMON")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .observedAt(LocalDateTime.now())
                .build();

        assertEquals(event1.getEventId(), event2.getEventId());
    }
}
