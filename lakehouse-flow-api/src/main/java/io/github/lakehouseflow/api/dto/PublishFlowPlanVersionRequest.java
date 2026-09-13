package io.github.lakehouseflow.api.dto;

/**
 * Request body for publishing a FlowPlan version.
 *
 * @param publishedBy user or system identity publishing the version
 */
public record PublishFlowPlanVersionRequest(String publishedBy) {
}
