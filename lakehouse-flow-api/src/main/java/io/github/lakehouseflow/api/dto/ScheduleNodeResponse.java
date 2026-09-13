package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * API response for a schedule node definition.
 *
 * @param id schedule node id
 * @param flowPlanVersionId owning FlowPlanVersion id
 * @param nodeCode stable node code
 * @param nodeName human-readable node name
 * @param nodeType scheduler-side node type
 * @param dependsOnNodes upstream node codes
 * @param inputDependencySpecJson input asset dependency conditions
 * @param outputAssetKey target asset expected to advance
 * @param confirmationPolicyJson node-level snapshot confirmation policy
 * @param sortOrder deterministic graph ordering hint
 * @param createdAt record creation timestamp
 * @param updatedAt record update timestamp
 */
public record ScheduleNodeResponse(
        Long id,
        Long flowPlanVersionId,
        String nodeCode,
        String nodeName,
        String nodeType,
        List<String> dependsOnNodes,
        Map<String, Object> inputDependencySpecJson,
        String outputAssetKey,
        Map<String, Object> confirmationPolicyJson,
        Integer sortOrder,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
