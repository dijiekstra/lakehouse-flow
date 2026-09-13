package io.github.lakehouseflow.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests low-cardinality FlowPlan inspection and trigger-decision metrics.
 */
class FlowPlanDecisionMetricsTest {

    /** Verify every plan inspection is counted under its bounded outcome tag. */
    @Test
    void recordInspectionCountsBoundedOutcome() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FlowPlanDecisionMetrics metrics = new FlowPlanDecisionMetrics(registry);

        metrics.recordInspection(FlowPlanInspectionOutcome.ASSET_UNMATCHED);

        assertEquals(1.0, registry.counter(
                "lakehouse.flow.scheduling.decision.plan.inspections",
                "outcome", "asset_unmatched").count());
    }

    /** Verify matched decision counters and timers share the stable decision tag. */
    @Test
    void recordDecisionCountsAndTimesEvaluation() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FlowPlanDecisionMetrics metrics = new FlowPlanDecisionMetrics(registry);

        metrics.recordDecision(FlowPlanTriggerDecision.BLOCKED_CONDITION, Duration.ofMillis(25));

        assertEquals(1.0, registry.counter(
                "lakehouse.flow.scheduling.decision.trigger.evaluations",
                "decision", "blocked_condition").count());
        assertEquals(1L, registry.timer(
                "lakehouse.flow.scheduling.decision.trigger.evaluation.duration",
                "decision", "blocked_condition").count());
    }
}
