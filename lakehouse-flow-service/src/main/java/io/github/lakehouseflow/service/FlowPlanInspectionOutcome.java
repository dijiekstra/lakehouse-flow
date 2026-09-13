package io.github.lakehouseflow.service;

/**
 * Low-cardinality result of inspecting one published FlowPlan for a snapshot trigger.
 */
public enum FlowPlanInspectionOutcome {

    /** The version's trigger policy does not allow natural snapshot progression. */
    TRIGGER_POLICY_FILTERED,

    /** No root dependency in the version references the changed asset. */
    ASSET_UNMATCHED,

    /** A root dependency references the changed asset and requires a trigger decision. */
    MATCHED
}
