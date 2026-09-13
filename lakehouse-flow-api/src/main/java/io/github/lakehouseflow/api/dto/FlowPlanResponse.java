package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * API response for a FlowPlan definition.
 *
 * @param id FlowPlan id
 * @param flowCode stable Flow code
 * @param flowName human-readable Flow name
 * @param flowSpaceCode lightweight sharing boundary
 * @param owner Flow owner
 * @param description human-readable description
 * @param status FlowPlan lifecycle status
 * @param currentVersion currently published version number
 * @param createdAt record creation timestamp
 * @param updatedAt record update timestamp
 */
public record FlowPlanResponse(
        Long id,
        String flowCode,
        String flowName,
        String flowSpaceCode,
        String owner,
        String description,
        String status,
        Integer currentVersion,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
