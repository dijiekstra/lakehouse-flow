package io.github.lakehouseflow.api.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Request body for creating a FlowPlan draft.
 *
 * @param flowCode stable Flow code
 * @param flowName human-readable Flow name
 * @param flowSpaceCode lightweight sharing boundary
 * @param owner Flow owner
 * @param description human-readable description
 */
public record CreateFlowPlanRequest(
        @NotBlank String flowCode,
        @NotBlank String flowName,
        String flowSpaceCode,
        String owner,
        String description) {
}
