package io.github.lakehouseflow.service;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests date-template resolution for task targets and dependency JSON.
 */
class SchedulingTemplateResolverTest {

    private final SchedulingTemplateResolver resolver = new SchedulingTemplateResolver();

    /**
     * Verify both supported date placeholders resolve in a scalar value.
     */
    @Test
    void resolveReplacesBusinessDatePlaceholders() {
        assertEquals(
                "orders.dt=2026-09-12/payments.dt=2026-09-12",
                resolver.resolve("orders.dt=${bizDate}/payments.dt=${biz_date}", LocalDate.of(2026, 9, 12)));
        assertNull(resolver.resolve(null, LocalDate.of(2026, 9, 12)));
    }

    /**
     * Verify nested maps and lists are copied and resolved recursively.
     */
    @Test
    void resolveMapResolvesNestedJsonValues() {
        Map<String, Object> result = resolver.resolveMap(
                Map.of("groups", List.of(Map.of("assetKey", "orders.dt=${bizDate}"))),
                LocalDate.of(2026, 9, 12));

        assertEquals(
                "orders.dt=2026-09-12",
                ((Map<?, ?>) ((List<?>) result.get("groups")).get(0)).get("assetKey"));
        assertEquals(Map.of(), resolver.resolveMap(null, LocalDate.of(2026, 9, 12)));
    }
}
