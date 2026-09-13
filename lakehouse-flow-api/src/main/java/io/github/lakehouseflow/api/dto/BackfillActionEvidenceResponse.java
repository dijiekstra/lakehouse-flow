package io.github.lakehouseflow.api.dto;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Backfill batch evidence joined to an action query.
 *
 * @param id batch id
 * @param batchKey stable batch idempotency key
 * @param actionKey action that created this batch
 * @param scopeType complete-flow or node-subgraph scope
 * @param entryNodeCodes nodes admitted without graph parents
 * @param selectedNodeCodes immutable nodes expanded for each date
 * @param startScheduleNodeId node-subgraph start node id
 * @param startNodeCode node-subgraph start node code
 * @param bizDateStart inclusive date range start
 * @param bizDateEnd inclusive date range end
 * @param cascadePolicy node cascade policy
 * @param progressionMode date progression mode
 * @param maxActiveDates active-date limit
 * @param skipPolicy snapshot-confirmed date skip policy
 * @param skippedDateCount dates omitted using snapshot evidence
 * @param skipEvidence date-keyed task evidence used for skips
 * @param sourceBackfillBatchId failed batch replaced by this batch
 * @param recoveryAttempt recovery depth
 * @param recoveryStrategy replacement strategy, or null for an original batch
 * @param status scheduler-side batch status
 * @param producedWorkflowCount generated workflow wrapper count
 * @param totalItemCount generated task intent count
 */
public record BackfillActionEvidenceResponse(
        Long id,
        String batchKey,
        String actionKey,
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
        Integer totalItemCount) {
}
