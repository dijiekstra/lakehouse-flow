package io.github.lakehouseflow.model;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests normalization of dependencyConditions payloads into DependencySpec.
 */
class DependencySpecTest {

    /**
     * Verify that a structured payload keeps operator, versions, target, and nested conditions.
     */
    @Test
    void parsesStructuredDependencySpec() {
        Map<String, Object> condition = new LinkedHashMap<>();
        condition.put("type", "SNAPSHOT_ID_GTE");
        condition.put("assetKey", "paimon.prod.ods_orders");
        condition.put("value", "100");

        Map<String, Object> rawSpec = new LinkedHashMap<>();
        rawSpec.put("operator", "OR");
        rawSpec.put("workflowVersion", "2");
        rawSpec.put("taskVersion", 3);
        rawSpec.put("targetAssetKey", "paimon.prod.dwd_orders");
        rawSpec.put("conditions", List.of(condition));

        DependencySpec spec = DependencySpec.from(rawSpec, "paimon.prod.default_asset");

        assertTrue(spec.usesOrOperator());
        assertFalse(spec.usesAndOperator());
        assertEquals(2, spec.workflowVersion());
        assertEquals(3, spec.taskVersion());
        assertEquals("paimon.prod.dwd_orders", spec.targetAssetKey());
        assertEquals(1, spec.conditions().size());
        assertEquals("SNAPSHOT_ID_GTE", spec.conditions().get(0).type());
        assertEquals("paimon.prod.ods_orders", spec.conditions().get(0).assetKey());
        assertEquals("100", spec.conditions().get(0).value());
    }

    /**
     * Verify that a flat legacy payload becomes a single condition with defaults.
     */
    @Test
    void defaultsFlatDependencySpecToSingleAndCondition() {
        Map<String, Object> rawSpec = new LinkedHashMap<>();
        rawSpec.put("type", "SNAPSHOT_EXISTS");

        DependencySpec spec = DependencySpec.from(rawSpec, "paimon.prod.ods_orders");

        assertTrue(spec.usesAndOperator());
        assertFalse(spec.usesOrOperator());
        assertEquals(1, spec.workflowVersion());
        assertEquals(1, spec.taskVersion());
        assertEquals(1, spec.conditions().size());
        assertEquals("SNAPSHOT_EXISTS", spec.conditions().get(0).type());
        assertEquals("paimon.prod.ods_orders", spec.conditions().get(0).assetKey());
    }

    /**
     * Verify that an empty payload yields no schedulable dependency conditions.
     */
    @Test
    void returnsEmptyConditionsForMissingSpec() {
        DependencySpec spec = DependencySpec.from(null, "paimon.prod.ods_orders");

        assertTrue(spec.usesAndOperator());
        assertEquals(0, spec.conditions().size());
    }
}
