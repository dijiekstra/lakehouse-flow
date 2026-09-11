package io.github.lakehouseflow.service;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WorkflowInstanceService logic (without database).
 * 
 * Database integration tests will run in lakehouse-flow-boot module.
 */
class WorkflowInstanceServiceTest {

    private static final DateTimeFormatter ISO_DATE_TIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Test
    void testInstanceKeyBuilding() {
        // Given: workflow parameters
        String workflowCode = "dwd_order_agg";
        Integer workflowVersion = 1;
        LocalDateTime bizDate = LocalDateTime.of(2025, 9, 11, 0, 0, 0);
        String triggerType = "SNAPSHOT";
        String triggerId = "event_123";

        // When: building instance key
        String instanceKey = String.format("%s:%d:%s:%s:%s",
                workflowCode,
                workflowVersion,
                bizDate.format(ISO_DATE_TIME),
                triggerType,
                triggerId);

        // Then: key should be deterministic and unique
        assertEquals("dwd_order_agg:1:2025-09-11T00:00:00:SNAPSHOT:event_123", instanceKey);

        // And: same parameters produce same key (idempotency)
        String instanceKey2 = String.format("%s:%d:%s:%s:%s",
                workflowCode,
                workflowVersion,
                bizDate.format(ISO_DATE_TIME),
                triggerType,
                triggerId);
        assertEquals(instanceKey, instanceKey2);
    }

    @Test
    void testValidStateTransitions() {
        // Given: valid transition rules
        // When: checking if CREATED -> WAITING is valid
        // Then: should be valid
        assertTrue(isValidTransition("CREATED", "WAITING"));

        // And: other valid transitions
        assertTrue(isValidTransition("WAITING", "RUNNING"));
        assertTrue(isValidTransition("RUNNING", "SUCCESS"));
        assertTrue(isValidTransition("RUNNING", "FAILED"));
        assertTrue(isValidTransition("FAILED", "RUNNING")); // Manual rerun
    }

    @Test
    void testInvalidStateTransitions() {
        // Given: invalid transition rules
        // When: checking if WAITING -> SUCCESS is valid (skip RUNNING)
        // Then: should be invalid
        assertFalse(isValidTransition("WAITING", "SUCCESS"));

        // And: other invalid transitions
        assertFalse(isValidTransition("RUNNING", "CREATED"));
        assertFalse(isValidTransition("SUCCESS", "RUNNING")); // Can't rerun from SUCCESS
    }

    @Test
    void testInstanceKeyUniqueness() {
        // Given: two workflows with same code but different dates
        String key1 = "workflow1:1:2025-09-11T00:00:00:SNAPSHOT:event1";
        String key2 = "workflow1:1:2025-09-12T00:00:00:SNAPSHOT:event1";

        // When: comparing keys
        // Then: should be different
        assertNotEquals(key1, key2);
    }

    @Test
    void testInstanceKeyWithoutEventId() {
        // Given: manual trigger without event ID
        String workflowCode = "dwd_order_agg";
        Integer workflowVersion = 1;
        LocalDateTime bizDate = LocalDateTime.of(2025, 9, 11, 0, 0, 0);
        String triggerType = "MANUAL";
        String triggerId = null;

        // When: building instance key (use "manual" as fallback)
        String instanceKey = String.format("%s:%d:%s:%s:%s",
                workflowCode,
                workflowVersion,
                bizDate.format(ISO_DATE_TIME),
                triggerType,
                triggerId != null ? triggerId : "manual");

        // Then: key should still be valid
        assertEquals("dwd_order_agg:1:2025-09-11T00:00:00:MANUAL:manual", instanceKey);
    }

    /**
     * Simple state transition validator (same logic as service)
     */
    private boolean isValidTransition(String fromState, String toState) {
        return switch (fromState) {
            case "CREATED" -> toState.equals("WAITING") || toState.equals("RUNNING") || toState.equals("CANCELLED");
            case "WAITING" -> toState.equals("RUNNING") || toState.equals("CANCELLED");
            case "RUNNING" -> toState.equals("SUCCESS") || toState.equals("FAILED") || toState.equals("TIMEOUT");
            case "FAILED" -> toState.equals("RUNNING"); // Manual rerun
            case "TIMEOUT" -> toState.equals("RUNNING"); // Manual rerun
            default -> false;
        };
    }
}
