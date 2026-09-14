package io.github.lakehouseflow.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;

import java.util.List;
import java.util.Map;

/**
 * Request body for adding a node to a FlowPlan version draft.
 *
 * @param nodeCode stable node code within the version
 * @param nodeName human-readable node name
 * @param nodeType scheduler-side node type
 * @param processingMode engine-neutral STREAMING or BATCH mode; defaults to BATCH
 * @param dependsOnNodes upstream node codes
 * @param inputDependencySpecJson input asset dependency conditions
 * @param outputAssetKey target asset expected to advance after downstream consumption
 * @param writerJobKey stable platform writer owning the output physical table
 * @param confirmationPolicyJson node-level snapshot confirmation policy
 * @param sortOrder deterministic graph ordering hint
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateScheduleNodeRequest(
        @NotBlank String nodeCode,
        @NotBlank String nodeName,
        @NotBlank String nodeType,
        String processingMode,
        List<String> dependsOnNodes,
        Map<String, Object> inputDependencySpecJson,
        String outputAssetKey,
        String writerJobKey,
        Map<String, Object> confirmationPolicyJson,
        Integer sortOrder) {
}
