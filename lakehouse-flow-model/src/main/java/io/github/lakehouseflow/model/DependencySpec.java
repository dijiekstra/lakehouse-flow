package io.github.lakehouseflow.model;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Normalized dependency DSL used by the scheduling decision loop.
 *
 * The raw persistence format remains JSONB on AssetDependency. This record is
 * the code-facing shape used by services so Map parsing stays in one place.
 */
public record DependencySpec(
        String operator,
        int workflowVersion,
        int taskVersion,
        String targetAssetKey,
        List<DependencyCondition> conditions) {

    /**
     * Build a normalized dependency spec from a JSON-like map.
     *
     * @param rawSpec dependencyConditions payload from AssetDependency
     * @param defaultAssetKey asset key inherited from AssetDependency
     * @return normalized spec with default versions and conditions
     */
    public static DependencySpec from(Map<String, Object> rawSpec, String defaultAssetKey) {
        if (rawSpec == null || rawSpec.isEmpty()) {
            return new DependencySpec("AND", 1, 1, null, List.of());
        }

        return new DependencySpec(
                stringValue(rawSpec.get("operator"), "AND").toUpperCase(Locale.ROOT),
                intValue(rawSpec.get("workflowVersion"), 1),
                intValue(rawSpec.get("taskVersion"), 1),
                stringValue(rawSpec.get("targetAssetKey"), null),
                conditionSpecs(rawSpec, defaultAssetKey));
    }

    /**
     * Return true when this dependency should use OR semantics.
     *
     * @return whether any condition can satisfy the dependency
     */
    public boolean usesOrOperator() {
        return "OR".equals(operator);
    }

    /**
     * Return true when this dependency should use AND semantics.
     *
     * @return whether all conditions must satisfy the dependency
     */
    public boolean usesAndOperator() {
        return "AND".equals(operator);
    }

    /**
     * Parse condition payloads from the normalized JSON structure.
     *
     * @param rawSpec raw dependencyConditions map
     * @param defaultAssetKey asset key inherited from AssetDependency
     * @return normalized condition list
     */
    @SuppressWarnings("unchecked")
    private static List<DependencyCondition> conditionSpecs(Map<String, Object> rawSpec, String defaultAssetKey) {
        Object conditions = rawSpec.get("conditions");
        if (conditions instanceof List<?> list && !list.isEmpty()) {
            return list.stream()
                    .filter(Map.class::isInstance)
                    .map(condition -> DependencyCondition.from((Map<String, Object>) condition, defaultAssetKey))
                    .toList();
        }
        return List.of(DependencyCondition.from(rawSpec, defaultAssetKey));
    }

    /**
     * Convert an arbitrary JSON value into an integer.
     *
     * @param value raw JSON value
     * @param defaultValue fallback value when the raw value is absent
     * @return parsed integer or fallback
     */
    private static int intValue(Object value, int defaultValue) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Integer.parseInt(text);
        }
        return defaultValue;
    }

    /**
     * Convert an arbitrary JSON value into a string.
     *
     * @param value raw JSON value
     * @param defaultValue fallback value when the raw value is absent
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
