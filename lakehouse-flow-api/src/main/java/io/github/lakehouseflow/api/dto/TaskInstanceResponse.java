package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * API response for a task scheduling instance and snapshot evidence.
 *
 * @param id task instance id
 * @param instanceKey idempotency key
 * @param workflowInstanceId owning workflow instance id
 * @param taskCode task or node code
 * @param taskVersion task version
 * @param flowPlanVersionId FlowPlanVersion that produced this scheduling instance
 * @param scheduleNodeId ScheduleNode that produced this scheduling instance
 * @param bizDate business date
 * @param state scheduler-side state
 * @param waitingReason waiting or failure explanation
 * @param targetAssetKey target asset used for snapshot confirmation
 * @param baselineSnapshotId snapshot observed before intent delivery
 * @param observedSnapshotId latest observed target snapshot
 * @param scheduledAt intent emission time
 * @param lastSnapshotCheckAt last snapshot evidence check time
 * @param createdAt record creation timestamp
 * @param updatedAt record update timestamp
 */
public record TaskInstanceResponse(
        Long id,
        String instanceKey,
        Long workflowInstanceId,
        String taskCode,
        Integer taskVersion,
        Long flowPlanVersionId,
        Long scheduleNodeId,
        LocalDateTime bizDate,
        String state,
        String waitingReason,
        String targetAssetKey,
        String baselineSnapshotId,
        String observedSnapshotId,
        LocalDateTime scheduledAt,
        LocalDateTime lastSnapshotCheckAt,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
