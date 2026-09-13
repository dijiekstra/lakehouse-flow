package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * API response for one FlowPlan version.
 *
 * @param id FlowPlanVersion id
 * @param flowPlanId owning FlowPlan id
 * @param flowCode denormalized Flow code
 * @param version version number
 * @param status version lifecycle status
 * @param graphJson graph metadata
 * @param dependencySpecJson Flow-level dependency defaults
 * @param triggerPolicyJson trigger policy
 * @param confirmationPolicyJson snapshot confirmation policy
 * @param concurrencyPolicyJson scheduler-side concurrency policy
 * @param publishedBy publisher identity
 * @param publishedAt publication timestamp
 * @param createdAt record creation timestamp
 * @param updatedAt record update timestamp
 */
public record FlowPlanVersionResponse(
        Long id,
        Long flowPlanId,
        String flowCode,
        Integer version,
        String status,
        Map<String, Object> graphJson,
        Map<String, Object> dependencySpecJson,
        Map<String, Object> triggerPolicyJson,
        Map<String, Object> confirmationPolicyJson,
        Map<String, Object> concurrencyPolicyJson,
        String publishedBy,
        LocalDateTime publishedAt,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
