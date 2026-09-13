package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * API response for target snapshot evidence related to one action task intent.
 *
 * @param backfillItemId owning backfill item id when applicable
 * @param backfillItemStatus scheduler-side backfill item state when applicable
 * @param taskRecordPresent whether the linked task record still exists
 * @param taskInstanceId task scheduling intent id
 * @param workflowInstanceId workflow wrapper id
 * @param flowPlanVersionId immutable FlowPlanVersion anchor
 * @param scheduleNodeId immutable ScheduleNode anchor
 * @param taskCode task or node code
 * @param bizDate business date represented by the intent
 * @param schedulingState scheduler-side task state
 * @param waitingReason latest scheduler-side waiting explanation
 * @param targetAssetKey asset whose progress confirms the result
 * @param baselineSnapshotId snapshot observed before intent delivery
 * @param observedSnapshotId snapshot observed by the latest check
 * @param snapshotAdvanced target snapshot progression result, or null before observation
 * @param lastSnapshotCheckAt latest evidence check timestamp
 * @param scheduledAt intent delivery timestamp
 * @param schedulingIntentId immutable published intent id
 * @param schedulingIntentKey scheduler-generated intent idempotency key
 * @param deliveryChannel infrastructure transport channel
 * @param deliveryStatus transport-only publication status
 * @param deliveryAttemptCount number of transport attempts
 * @param deliveryLastError latest transport error, or null
 * @param deliveryNextAttemptAt earliest next retry timestamp, or null
 * @param deliveryDeadLetteredAt terminal transport dead-letter timestamp, or null
 * @param publishedAt time the intent became available through its channel
 * @param updatedAt task evidence update timestamp
 */
public record TaskSnapshotEvidenceResponse(
        Long backfillItemId,
        String backfillItemStatus,
        boolean taskRecordPresent,
        Long taskInstanceId,
        Long workflowInstanceId,
        Long flowPlanVersionId,
        Long scheduleNodeId,
        String taskCode,
        LocalDateTime bizDate,
        String schedulingState,
        String waitingReason,
        String targetAssetKey,
        String baselineSnapshotId,
        String observedSnapshotId,
        Boolean snapshotAdvanced,
        LocalDateTime lastSnapshotCheckAt,
        LocalDateTime scheduledAt,
        Long schedulingIntentId,
        String schedulingIntentKey,
        String deliveryChannel,
        String deliveryStatus,
        Integer deliveryAttemptCount,
        String deliveryLastError,
        LocalDateTime deliveryNextAttemptAt,
        LocalDateTime deliveryDeadLetteredAt,
        LocalDateTime publishedAt,
        LocalDateTime updatedAt) {
}
