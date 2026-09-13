package io.github.lakehouseflow.api.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * List-safe API summary for one scheduler-side action.
 *
 * @param id action record id
 * @param actionKey caller-provided idempotency key
 * @param actionType scheduling action type
 * @param scopeType action target scope
 * @param status action processing status
 * @param workflowCode workflow definition code
 * @param workflowVersion workflow definition version
 * @param flowPlanVersionId immutable FlowPlanVersion anchor
 * @param scheduleNodeId immutable ScheduleNode anchor
 * @param workflowInstanceId targeted workflow instance
 * @param taskInstanceId targeted task instance
 * @param producedWorkflowInstanceId primary workflow instance affected or produced
 * @param producedTaskInstanceId primary task instance affected or produced
 * @param bizDateStart inclusive action date range start
 * @param bizDateEnd inclusive action date range end
 * @param requestedBy requester identity copied for audit
 * @param reason human-readable request reason
 * @param resultMessage scheduler-side action result
 * @param createdAt action creation timestamp
 * @param updatedAt action update timestamp
 */
public record SchedulingActionSummaryResponse(
        Long id,
        String actionKey,
        String actionType,
        String scopeType,
        String status,
        String workflowCode,
        Integer workflowVersion,
        Long flowPlanVersionId,
        Long scheduleNodeId,
        Long workflowInstanceId,
        Long taskInstanceId,
        Long producedWorkflowInstanceId,
        Long producedTaskInstanceId,
        LocalDate bizDateStart,
        LocalDate bizDateEnd,
        String requestedBy,
        String reason,
        String resultMessage,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
