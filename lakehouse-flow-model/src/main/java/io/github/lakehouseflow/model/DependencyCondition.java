package io.github.lakehouseflow.model;

import java.util.Map;

/**
 * One normalized input condition from a published FlowPlan dependency specification.
 */
public record DependencyCondition(
        String type,
        String assetKey,
        String value) {

    /**
     * Build a condition from a JSON-like map.
     *
     * @param rawCondition condition payload from a FlowPlan or ScheduleNode dependency specification
     * @param defaultAssetKey optional asset key inherited from the enclosing specification
     * @return normalized condition with stable defaults
     */
    public static DependencyCondition from(Map<String, Object> rawCondition, String defaultAssetKey) {
        return new DependencyCondition(
                stringValue(rawCondition.get("type"), "SNAPSHOT_EXISTS"),
                stringValue(rawCondition.get("assetKey"), defaultAssetKey),
                stringValue(rawCondition.get("value"), null));
    }

    /**
     * Convert an arbitrary JSON value into a string.
     *
     * @param value raw JSON value
     * @param defaultValue fallback value when the raw value is null
     * @return string representation or fallback
     */
    private static String stringValue(Object value, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof String text) {
            return text;
        }
        return value.toString();
    }
}
