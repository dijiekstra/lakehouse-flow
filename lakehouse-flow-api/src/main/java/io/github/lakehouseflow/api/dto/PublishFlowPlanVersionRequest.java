package io.github.lakehouseflow.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Request body for publishing a FlowPlan version.
 *
 * @param publishedBy user or system identity publishing the version
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PublishFlowPlanVersionRequest(String publishedBy) {
}
