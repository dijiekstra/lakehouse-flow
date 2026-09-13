package io.github.lakehouseflow.api.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * API response for a node/date item expanded from a backfill batch.
 *
 * @param id item id
 * @param backfillBatchId owning batch id
 * @param bizDate business date covered by this item
 * @param flowPlanVersionId published FlowPlanVersion used for expansion
 * @param scheduleNodeId ScheduleNode generated for this item
 * @param nodeCode generated node code
 * @param targetAssetKey target asset expected to advance after delivery
 * @param workflowInstanceId generated workflow wrapper id
 * @param taskInstanceId generated task intent id
 * @param status scheduler-side item status
 * @param createdAt record creation timestamp
 * @param updatedAt record update timestamp
 */
public record BackfillItemResponse(
        Long id,
        Long backfillBatchId,
        LocalDate bizDate,
        Long flowPlanVersionId,
        Long scheduleNodeId,
        String nodeCode,
        String targetAssetKey,
        Long workflowInstanceId,
        Long taskInstanceId,
        String status,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
