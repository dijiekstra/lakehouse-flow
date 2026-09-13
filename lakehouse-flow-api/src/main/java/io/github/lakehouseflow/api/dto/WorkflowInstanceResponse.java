package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * API response for a workflow scheduling instance.
 *
 * @param id workflow instance id
 * @param instanceKey idempotency key
 * @param workflowCode workflow code
 * @param workflowVersion workflow version
 * @param flowPlanVersionId FlowPlanVersion that produced this scheduling instance
 * @param bizDate business date
 * @param triggerType trigger type
 * @param triggerEventId trigger event id or action key
 * @param triggerReason human-readable trigger reason
 * @param state scheduler-side state
 * @param scheduledAt intent emission time
 * @param lastSnapshotCheckAt last snapshot evidence check time
 * @param createdAt record creation timestamp
 * @param updatedAt record update timestamp
 */
public record WorkflowInstanceResponse(
        Long id,
        String instanceKey,
        String workflowCode,
        Integer workflowVersion,
        Long flowPlanVersionId,
        LocalDateTime bizDate,
        String triggerType,
        String triggerEventId,
        String triggerReason,
        String state,
        LocalDateTime scheduledAt,
        LocalDateTime lastSnapshotCheckAt,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
