package io.github.lakehouseflow.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * Request body for creating a FlowPlan version draft.
 *
 * @param version requested version, or null to use currentVersion + 1
 * @param graphJson graph metadata
 * @param dependencySpecJson Flow-level dependency defaults
 * @param triggerPolicyJson trigger policy
 * @param confirmationPolicyJson snapshot confirmation policy
 * @param concurrencyPolicyJson scheduler-side concurrency policy
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateFlowPlanVersionRequest(
        Integer version,
        Map<String, Object> graphJson,
        Map<String, Object> dependencySpecJson,
        Map<String, Object> triggerPolicyJson,
        Map<String, Object> confirmationPolicyJson,
        Map<String, Object> concurrencyPolicyJson) {
}
