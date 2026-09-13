package io.github.lakehouseflow.service;

/**
 * Low-cardinality outcome of evaluating a matched FlowPlan snapshot trigger.
 */
public enum FlowPlanTriggerDecision {

    /** A new workflow and its node scheduling decisions were emitted. */
    EMITTED,

    /** At least one root snapshot dependency is not yet satisfied. */
    BLOCKED_CONDITION,

    /** The durable trigger key already exists and no duplicate instance was emitted. */
    DEDUPLICATED,

    /** Evaluation failed and the surrounding ingestion transaction must fail closed. */
    FAILED
}
