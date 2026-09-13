package io.github.lakehouseflow.api.dto;

import java.util.List;

/**
 * API response for scheduler-side action processing results.
 *
 * @param actionKey action idempotency key
 * @param actionType scheduling action type
 * @param status action processing status
 * @param message human-readable result message
 * @param workflowInstanceIds workflow instances created or affected
 * @param taskInstanceId task instance affected by the action
 */
public record SchedulingActionResponse(
        String actionKey,
        String actionType,
        String status,
        String message,
        List<Long> workflowInstanceIds,
        Long taskInstanceId) {
}
