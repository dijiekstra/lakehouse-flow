package io.github.lakehouseflow.common;

/**
 * Node types supported by a FlowPlan scheduling graph.
 *
 * Nodes describe scheduler-side dependency and target asset semantics. They do
 * not imply that Lakehouse Flow owns an executor implementation for the node.
 */
public final class ScheduleNodeTypes {

    public static final String ASSET_OUTPUT = "ASSET_OUTPUT";
    public static final String CHECKPOINT = "CHECKPOINT";
    public static final String SUB_FLOW = "SUB_FLOW";

    /**
     * Prevent utility class instantiation.
     */
    private ScheduleNodeTypes() {
    }

    /**
     * Check whether the node type should declare an output asset for snapshot confirmation.
     *
     * @param nodeType schedule node type
     * @return true when the node is expected to bind to a target asset
     */
    public static boolean requiresOutputAsset(String nodeType) {
        return ASSET_OUTPUT.equals(nodeType);
    }
}
