package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Read-only API response for one committed scheduling intent and its delivery evidence.
 *
 * @param intentId immutable intent id
 * @param contractVersion downstream instruction contract version
 * @param intentKey scheduler-generated idempotency key
 * @param taskInstanceId task scheduling instance id
 * @param workflowInstanceId owning workflow instance id
 * @param triggerType scheduling trigger mode
 * @param backfillBatchId owning backfill batch id, or null
 * @param backfillItemId owning backfill item id, or null
 * @param taskCode task or node code consumed by downstream systems
 * @param taskVersion task definition version
 * @param flowPlanVersionId FlowPlanVersion that produced this intent
 * @param scheduleNodeId ScheduleNode that produced this intent
 * @param bizDate business date for the scheduling decision
 * @param targetAssetKey managed asset used for snapshot confirmation
 * @param baselineSnapshotId snapshot frozen before publication
 * @param writerJobKey stable writer owning the target physical table
 * @param writerEpoch fenced writer generation
 * @param processingMode engine-neutral STREAMING or BATCH mode
 * @param inputSnapshotVector complete frozen input evidence
 * @param instructionPayload complete immutable downstream instruction
 * @param deliveryChannel infrastructure transport channel
 * @param deliveryDestination channel-specific destination
 * @param deliveryStatus transport-only publication status
 * @param deliveryAttemptCount number of transport attempts
 * @param deliveryLastError latest transport error, or null
 * @param deliveryLastAttemptAt latest transport attempt timestamp, or null
 * @param deliveryNextAttemptAt earliest next retry timestamp, or null
 * @param deliveryDeadLetteredAt terminal transport dead-letter timestamp, or null
 * @param publishedAt time the intent became available through the channel
 * @param createdAt immutable intent creation timestamp
 */
public record TaskSchedulingIntentResponse(
        Long intentId,
        String contractVersion,
        String intentKey,
        Long taskInstanceId,
        Long workflowInstanceId,
        String triggerType,
        Long backfillBatchId,
        Long backfillItemId,
        String taskCode,
        Integer taskVersion,
        Long flowPlanVersionId,
        Long scheduleNodeId,
        LocalDateTime bizDate,
        String targetAssetKey,
        String baselineSnapshotId,
        String writerJobKey,
        Long writerEpoch,
        String processingMode,
        List<Map<String, Object>> inputSnapshotVector,
        Map<String, Object> instructionPayload,
        String deliveryChannel,
        String deliveryDestination,
        String deliveryStatus,
        Integer deliveryAttemptCount,
        String deliveryLastError,
        LocalDateTime deliveryLastAttemptAt,
        LocalDateTime deliveryNextAttemptAt,
        LocalDateTime deliveryDeadLetteredAt,
        LocalDateTime publishedAt,
        LocalDateTime createdAt) {
}
