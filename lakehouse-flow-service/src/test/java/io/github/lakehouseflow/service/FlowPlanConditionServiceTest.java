package io.github.lakehouseflow.service;

import io.github.lakehouseflow.model.EvaluationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests grouped FlowPlan dependency evaluation with business-date resolution.
 */
@ExtendWith(MockitoExtension.class)
class FlowPlanConditionServiceTest {

    private static final LocalDate BIZ_DATE = LocalDate.of(2026, 9, 12);

    @Mock
    private ConditionEvaluator conditionEvaluator;

    @Mock
    private SchedulingTemplateResolver schedulingTemplateResolver;

    @InjectMocks
    private FlowPlanConditionService flowPlanConditionService;

    /**
     * Verify an empty external dependency has no scheduling gate.
     */
    @Test
    void evaluateTreatsEmptySpecAsSatisfied() {
        assertTrue(flowPlanConditionService.evaluate(Map.of(), BIZ_DATE).getSatisfied());
    }

    /**
     * Verify grouped operators aggregate current AssetState condition evidence.
     */
    @Test
    void evaluateSupportsGroupedAndOrConditions() {
        Map<String, Object> first = Map.of(
                "type", "SNAPSHOT_ADVANCED",
                "assetKey", "orders.dt=2026-09-12");
        Map<String, Object> second = Map.of(
                "type", "QUALITY_PASSED",
                "assetKey", "orders.dt=2026-09-12");
        Map<String, Object> resolved = Map.of(
                "groupOperator", "OR",
                "groups", List.of(
                        Map.of("operator", "AND", "conditions", List.of(first, second)),
                        Map.of("operator", "AND", "conditions", List.of(first))));
        when(schedulingTemplateResolver.resolveMap(resolved, BIZ_DATE)).thenReturn(resolved);
        when(conditionEvaluator.evaluateCondition("SNAPSHOT_EXISTS", "orders.dt=2026-09-12", null))
                .thenReturn(EvaluationResult.satisfied("snapshot exists"));
        when(conditionEvaluator.evaluateCondition("QUALITY_PASSED", "orders.dt=2026-09-12", null))
                .thenReturn(EvaluationResult.unsatisfied("quality waiting"));

        EvaluationResult result = flowPlanConditionService.evaluate(resolved, BIZ_DATE);

        assertTrue(result.getSatisfied());
        verify(conditionEvaluator).evaluateCondition("QUALITY_PASSED", "orders.dt=2026-09-12", null);
    }

    /**
     * Verify failed flat conditions retain a useful waiting reason.
     */
    @Test
    void evaluateReturnsWaitingReasonForUnsatisfiedFlatSpec() {
        Map<String, Object> resolved = Map.of(
                "operator", "AND",
                "conditions", List.of(Map.of(
                        "type", "SNAPSHOT_EXISTS",
                        "assetKey", "orders.dt=2026-09-12")));
        when(schedulingTemplateResolver.resolveMap(resolved, BIZ_DATE)).thenReturn(resolved);
        when(conditionEvaluator.evaluateCondition("SNAPSHOT_EXISTS", "orders.dt=2026-09-12", null))
                .thenReturn(EvaluationResult.unsatisfied("snapshot waiting"));

        EvaluationResult result = flowPlanConditionService.evaluate(resolved, BIZ_DATE);

        assertFalse(result.getSatisfied());
        assertTrue(result.getWaitingReason().contains("snapshot waiting"));
    }

    /**
     * Verify asset-reference lookup traverses grouped conditions.
     */
    @Test
    void referencesAssetFindsResolvedNestedCondition() {
        Map<String, Object> raw = Map.of(
                "groups", List.of(Map.of(
                        "conditions", List.of(Map.of("assetKey", "orders.dt=${bizDate}")))));
        Map<String, Object> resolved = Map.of(
                "groups", List.of(Map.of(
                        "conditions", List.of(Map.of("assetKey", "orders.dt=2026-09-12")))));
        when(schedulingTemplateResolver.resolveMap(raw, BIZ_DATE)).thenReturn(resolved);

        assertTrue(flowPlanConditionService.referencesAsset(raw, BIZ_DATE, "orders.dt=2026-09-12"));
        assertFalse(flowPlanConditionService.referencesAsset(raw, BIZ_DATE, "payments.dt=2026-09-12"));
    }
}
