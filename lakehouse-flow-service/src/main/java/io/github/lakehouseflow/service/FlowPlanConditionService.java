package io.github.lakehouseflow.service;

import io.github.lakehouseflow.model.DependencyCondition;
import io.github.lakehouseflow.model.EvaluationResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Evaluates FlowPlan input dependency JSON after resolving business-date templates.
 *
 * Both the documented grouped DSL and the legacy flat `conditions` shape are
 * accepted so published plans can migrate without changing scheduling truth.
 */
@Service
@RequiredArgsConstructor
public class FlowPlanConditionService {

    private final ConditionEvaluator conditionEvaluator;
    private final SchedulingTemplateResolver schedulingTemplateResolver;

    /**
     * Evaluate a FlowPlan dependency specification against current AssetState evidence.
     *
     * An empty specification has no external gate and is therefore satisfied.
     * `SNAPSHOT_ADVANCED` means that the date-resolved asset has an observed
     * snapshot; the snapshot event itself remains the trigger and idempotency key.
     *
     * @param rawSpec persisted dependency JSON
     * @param bizDate business date used for template resolution
     * @return aggregate condition result
     */
    public EvaluationResult evaluate(Map<String, Object> rawSpec, LocalDate bizDate) {
        return evaluateWithEvidence(rawSpec, bizDate).aggregate();
    }

    /**
     * Evaluate a dependency specification once and retain satisfied asset evidence for intent freezing.
     *
     * @param rawSpec persisted dependency JSON
     * @param bizDate business date used for template resolution
     * @return aggregate decision and deterministic satisfied condition evidence
     */
    DependencyEvaluation evaluateWithEvidence(Map<String, Object> rawSpec, LocalDate bizDate) {
        if (rawSpec == null || rawSpec.isEmpty()) {
            return new DependencyEvaluation(
                    EvaluationResult.satisfied("No external asset dependency"),
                    List.of());
        }

        Map<String, Object> spec = schedulingTemplateResolver.resolveMap(rawSpec, bizDate);
        List<GroupResult> groups = evaluateGroups(spec);
        if (groups.isEmpty()) {
            return new DependencyEvaluation(
                    EvaluationResult.unsatisfied("Dependency conditions are empty"),
                    List.of());
        }

        String groupOperator = stringValue(spec.get("groupOperator"), "OR").toUpperCase(Locale.ROOT);
        boolean satisfied = combine(groupOperator, groups.stream().map(GroupResult::satisfied).toList());
        String reason = satisfied
                ? "FlowPlan input dependency conditions satisfied"
                : groups.stream()
                        .filter(group -> !group.satisfied())
                        .map(GroupResult::reason)
                        .filter(value -> value != null && !value.isBlank())
                        .findFirst()
                        .orElse("FlowPlan input dependency conditions are not satisfied");
        EvaluationResult aggregate = EvaluationResult.builder()
                .satisfied(satisfied)
                .waitingReason(satisfied ? null : reason)
                .description(reason)
                .evaluatedAt(System.currentTimeMillis())
                .build();
        if (!satisfied) {
            return new DependencyEvaluation(aggregate, List.of());
        }
        List<EvaluationResult> evidence = groups.stream()
                .filter(group -> "AND".equals(groupOperator) || group.satisfied())
                .flatMap(group -> group.results().stream())
                .filter(result -> Boolean.TRUE.equals(result.getSatisfied()))
                .filter(result -> result.getAssetKey() != null)
                .toList();
        return new DependencyEvaluation(aggregate, evidence);
    }

    /**
     * Check whether a dependency specification references a changed asset.
     *
     * @param rawSpec persisted dependency JSON
     * @param bizDate business date used for template resolution
     * @param assetKey changed asset key
     * @return true when at least one condition targets the asset
     */
    public boolean referencesAsset(Map<String, Object> rawSpec, LocalDate bizDate, String assetKey) {
        if (rawSpec == null || rawSpec.isEmpty() || assetKey == null) {
            return false;
        }
        Map<String, Object> spec = schedulingTemplateResolver.resolveMap(rawSpec, bizDate);
        return conditionMaps(spec).stream()
                .map(condition -> stringValue(condition.get("assetKey"), null))
                .anyMatch(assetKey::equals);
    }

    /**
     * Evaluate grouped or flat conditions into group-level results.
     *
     * @param spec date-resolved dependency specification
     * @return evaluated groups
     */
    private List<GroupResult> evaluateGroups(Map<String, Object> spec) {
        Object groupsValue = spec.get("groups");
        if (groupsValue instanceof List<?> groups && !groups.isEmpty()) {
            return groups.stream()
                    .filter(Map.class::isInstance)
                    .map(this::castMap)
                    .map(this::evaluateGroup)
                    .toList();
        }
        return List.of(evaluateGroup(spec));
    }

    /**
     * Evaluate one AND/OR condition group.
     *
     * @param group dependency group
     * @return group-level result
     */
    private GroupResult evaluateGroup(Map<String, Object> group) {
        String operator = stringValue(group.get("operator"), "AND").toUpperCase(Locale.ROOT);
        List<EvaluationResult> results = conditionMaps(group).stream()
                .map(condition -> evaluateCondition(DependencyCondition.from(condition, null)))
                .toList();
        if (results.isEmpty()) {
            return new GroupResult(false, "Dependency group has no conditions", List.of());
        }
        boolean satisfied = combine(
                operator,
                results.stream().map(result -> Boolean.TRUE.equals(result.getSatisfied())).toList());
        String reason = results.stream()
                .filter(result -> !Boolean.TRUE.equals(result.getSatisfied()))
                .map(EvaluationResult::getWaitingReason)
                .filter(value -> value != null && !value.isBlank())
                .findFirst()
                .orElse(satisfied ? "Dependency group satisfied" : "Dependency group is not satisfied");
        return new GroupResult(satisfied, reason, results);
    }

    /**
     * Evaluate one normalized condition using AssetState evidence.
     *
     * @param condition normalized condition
     * @return condition result
     */
    private EvaluationResult evaluateCondition(DependencyCondition condition) {
        String conditionType = "SNAPSHOT_ADVANCED".equals(condition.type())
                ? "SNAPSHOT_EXISTS"
                : condition.type();
        return conditionEvaluator.evaluateCondition(conditionType, condition.assetKey(), condition.value());
    }

    /**
     * Extract condition maps from a group or legacy single-condition shape.
     *
     * @param container JSON object containing conditions
     * @return normalized condition maps
     */
    private List<Map<String, Object>> conditionMaps(Map<String, Object> container) {
        Object conditionsValue = container.get("conditions");
        if (conditionsValue instanceof List<?> conditions) {
            return conditions.stream()
                    .filter(Map.class::isInstance)
                    .map(this::castMap)
                    .toList();
        }
        if (container.containsKey("assetKey") || container.containsKey("type")) {
            return List.of(container);
        }

        Object groupsValue = container.get("groups");
        if (groupsValue instanceof List<?> groups) {
            List<Map<String, Object>> flattened = new ArrayList<>();
            groups.stream()
                    .filter(Map.class::isInstance)
                    .map(this::castMap)
                    .map(this::conditionMaps)
                    .forEach(flattened::addAll);
            return flattened;
        }
        return List.of();
    }

    /**
     * Combine boolean values with a strict AND or OR operator.
     *
     * @param operator logical operator
     * @param values values to combine
     * @return aggregate result
     */
    private boolean combine(String operator, List<Boolean> values) {
        return switch (operator) {
            case "AND" -> values.stream().allMatch(Boolean.TRUE::equals);
            case "OR" -> values.stream().anyMatch(Boolean.TRUE::equals);
            default -> throw new IllegalArgumentException("Unsupported dependency operator: " + operator);
        };
    }

    /**
     * Cast a JSON-compatible map while keeping unchecked conversion local.
     *
     * @param value raw map value
     * @return string-keyed map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    /**
     * Convert a JSON value into text with a fallback.
     *
     * @param value JSON value
     * @param defaultValue fallback text
     * @return normalized text
     */
    private String stringValue(Object value, String defaultValue) {
        return value == null ? defaultValue : value.toString();
    }

    /**
     * Internal result for one dependency group.
     *
     * @param satisfied whether the group passed
     * @param reason group evaluation reason
     * @param results evaluated conditions in definition order
     */
    private record GroupResult(boolean satisfied, String reason, List<EvaluationResult> results) {
    }

    /**
     * Internal aggregate and evidence result shared with mixed-DAG input freezing.
     *
     * @param aggregate dependency decision
     * @param satisfiedEvidence condition evidence that made the decision true
     */
    record DependencyEvaluation(
            EvaluationResult aggregate,
            List<EvaluationResult> satisfiedEvidence) {
    }
}
