package io.github.lakehouseflow.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Locale;

/**
 * Low-cardinality Micrometer instrumentation for natural FlowPlan trigger decisions.
 *
 * <p>Flow, asset, snapshot, and trigger identifiers deliberately stay out of tags. Durable
 * high-cardinality evidence remains in TriggerHistory and structured decision logs.
 */
@Component
public class FlowPlanDecisionMetrics {

    private static final String METRIC_PREFIX = "lakehouse.flow.scheduling.decision";

    private final MeterRegistry meterRegistry;

    /**
     * Create scheduling-decision metrics backed by the application registry.
     *
     * @param meterRegistry application meter registry
     */
    public FlowPlanDecisionMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Record why one published FlowPlan was or was not selected for evaluation.
     *
     * @param outcome bounded inspection result
     */
    public void recordInspection(FlowPlanInspectionOutcome outcome) {
        Counter.builder(METRIC_PREFIX + ".plan.inspections")
                .tag("outcome", tag(outcome))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Record one matched FlowPlan decision attempt and its evaluation duration.
     *
     * <p>The durable TriggerHistory remains the committed audit source when a surrounding
     * ingestion transaction later rolls back.
     *
     * @param decision bounded trigger decision result
     * @param duration decision evaluation duration
     */
    public void recordDecision(FlowPlanTriggerDecision decision, Duration duration) {
        String decisionTag = tag(decision);
        Counter.builder(METRIC_PREFIX + ".trigger.evaluations")
                .tag("decision", decisionTag)
                .register(meterRegistry)
                .increment();
        Timer.builder(METRIC_PREFIX + ".trigger.evaluation.duration")
                .tag("decision", decisionTag)
                .register(meterRegistry)
                .record(duration);
    }

    /** Convert one required enum value into a stable lower-case metric tag. */
    private String tag(Enum<?> value) {
        if (value == null) {
            throw new IllegalArgumentException("metric outcome must not be null");
        }
        return value.name().toLowerCase(Locale.ROOT);
    }
}
