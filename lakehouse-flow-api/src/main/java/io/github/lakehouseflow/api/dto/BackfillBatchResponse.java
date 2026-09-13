package io.github.lakehouseflow.api.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * API response for a scheduler-side backfill batch.
 *
 * @param id batch id
 * @param batchKey stable batch idempotency key
 * @param actionKey scheduling action key that created the batch
 * @param flowPlanVersionId published FlowPlanVersion used for expansion
 * @param workflowCode workflow code copied from the FlowPlanVersion
 * @param workflowVersion workflow version copied from the FlowPlanVersion
 * @param scopeType complete-flow or node-subgraph scope
 * @param entryNodeCodes nodes allowed to start each admitted business date
 * @param selectedNodeCodes immutable nodes expanded for every business date
 * @param startScheduleNodeId ScheduleNode where a node-subgraph expansion started
 * @param startNodeCode node code where a node-subgraph expansion started
 * @param bizDateStart inclusive start business date
 * @param bizDateEnd inclusive end business date
 * @param cascadePolicy cascade policy used during expansion
 * @param progressionMode business-date scheduling progression mode
 * @param maxActiveDates finite active-date limit, or null for unrestricted parallel progression
 * @param skipPolicy whole-date snapshot confirmation skip policy
 * @param skippedDateCount number of dates omitted with complete confirmation evidence
 * @param skipEvidence date-keyed node/task snapshot confirmation evidence
 * @param sourceBackfillBatchId failed batch replaced by this batch, or null for an original request
 * @param recoveryAttempt recovery depth in the replacement chain
 * @param recoveryStrategy replacement strategy, or null for an original batch
 * @param status scheduler-side batch status
 * @param producedWorkflowCount generated workflow wrapper count
 * @param totalItemCount generated task intent count
 * @param requestedBy user or system that requested the backfill
 * @param reason human-readable request reason
 * @param createdAt record creation timestamp
 * @param updatedAt record update timestamp
 */
public record BackfillBatchResponse(
        Long id,
        String batchKey,
        String actionKey,
        Long flowPlanVersionId,
        String workflowCode,
        Integer workflowVersion,
        String scopeType,
        List<String> entryNodeCodes,
        List<String> selectedNodeCodes,
        Long startScheduleNodeId,
        String startNodeCode,
        LocalDate bizDateStart,
        LocalDate bizDateEnd,
        String cascadePolicy,
        String progressionMode,
        Integer maxActiveDates,
        String skipPolicy,
        Integer skippedDateCount,
        Map<String, Object> skipEvidence,
        Long sourceBackfillBatchId,
        Integer recoveryAttempt,
        String recoveryStrategy,
        String status,
        Integer producedWorkflowCount,
        Integer totalItemCount,
        String requestedBy,
        String reason,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
