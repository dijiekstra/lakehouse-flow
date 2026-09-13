package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingActionTypes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.SchedulingActionRepository;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.SchedulingAction;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.model.TaskInstance;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Read-only service for action audit and target snapshot evidence queries.
 *
 * Action state describes whether Lakehouse Flow accepted and applied a
 * scheduler-side command. Task evidence separately shows whether the target
 * asset snapshot advanced; no executor runtime state is inferred here.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SchedulingActionQueryService {

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 200;
    private static final String SCOPE_BACKFILL_BATCH = "BACKFILL_BATCH";

    private final SchedulingActionRepository schedulingActionRepository;
    private final BackfillBatchRepository backfillBatchRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final SchedulingIntentRepository schedulingIntentRepository;
    private final SchedulingIntentDeliveryRepository schedulingIntentDeliveryRepository;

    /**
     * Summary of one durable scheduler-side action.
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
    public record ActionSummaryView(
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

    /**
     * Backfill batch evidence attached to an action detail response.
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
    public record BackfillBatchEvidenceView(
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

    /**
     * Snapshot evidence for one task scheduling intent related to an action.
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
     * @param snapshotAdvanced derived target snapshot progression result, or null before observation
     * @param lastSnapshotCheckAt latest evidence check timestamp
     * @param scheduledAt intent delivery timestamp
     * @param schedulingIntentId immutable published intent id
     * @param schedulingIntentKey scheduler-generated intent idempotency key
     * @param deliveryChannel transport channel used for publication
     * @param deliveryStatus transport-only publication status
     * @param deliveryAttemptCount number of transport attempts
     * @param deliveryLastError latest transport error, or null
     * @param deliveryNextAttemptAt earliest next retry timestamp, or null
     * @param deliveryDeadLetteredAt terminal transport dead-letter timestamp, or null
     * @param publishedAt time the intent became available through its channel
     * @param updatedAt task evidence update timestamp
     */
    public record TaskSnapshotEvidenceView(
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

    /**
     * Optional outbox evidence joined to task-level snapshot evidence.
     *
     * @param intentId immutable scheduling intent id
     * @param intentKey scheduler-generated idempotency key
     * @param channel infrastructure transport channel
     * @param status transport-only publication status
     * @param attemptCount number of transport attempts
     * @param lastError latest transport error, or null
     * @param nextAttemptAt earliest next retry timestamp, or null
     * @param deadLetteredAt terminal transport dead-letter timestamp, or null
     * @param publishedAt publication timestamp
     */
    private record IntentPublicationEvidence(
            Long intentId,
            String intentKey,
            String channel,
            String status,
            Integer attemptCount,
            String lastError,
            LocalDateTime nextAttemptAt,
            LocalDateTime deadLetteredAt,
            LocalDateTime publishedAt) {
    }

    /**
     * Complete action audit view with request, batch, and snapshot evidence.
     *
     * @param action action summary
     * @param targetAssetKey action-level target asset when applicable
     * @param targetSnapshotId action-level target snapshot when applicable
     * @param requestPayload structured action-specific request payload
     * @param backfillBatch related batch evidence when applicable
     * @param snapshotEvidence task-level target snapshot evidence
     */
    public record ActionDetailView(
            ActionSummaryView action,
            String targetAssetKey,
            String targetSnapshotId,
            Map<String, Object> requestPayload,
            BackfillBatchEvidenceView backfillBatch,
            List<TaskSnapshotEvidenceView> snapshotEvidence) {
    }

    /**
     * Find one action and join it to its batch and target snapshot evidence.
     *
     * @param actionKey caller-provided action idempotency key
     * @return detailed action view when present
     */
    public Optional<ActionDetailView> findActionByKey(String actionKey) {
        if (isBlank(actionKey)) {
            return Optional.empty();
        }
        return schedulingActionRepository.findByActionKey(actionKey.trim())
                .map(this::toDetailView);
    }

    /**
     * Search recent actions by Flow, published version, or node anchor.
     *
     * At least one anchor is required so this operational endpoint cannot
     * accidentally become an unbounded global action-table scan.
     *
     * @param workflowCode workflow code filter, or blank
     * @param flowPlanVersionId published FlowPlanVersion id filter, or null
     * @param scheduleNodeId ScheduleNode id filter, or null
     * @param limit maximum result count, defaulting to 50 and capped at 200
     * @return newest matching action summaries
     */
    public List<ActionSummaryView> searchActions(
            String workflowCode,
            Long flowPlanVersionId,
            Long scheduleNodeId,
            Integer limit) {

        String normalizedWorkflowCode = normalizeText(workflowCode);
        validatePositiveId(flowPlanVersionId, "flowPlanVersionId");
        validatePositiveId(scheduleNodeId, "scheduleNodeId");
        if (normalizedWorkflowCode == null && flowPlanVersionId == null && scheduleNodeId == null) {
            throw new IllegalArgumentException(
                    "At least one of workflowCode, flowPlanVersionId, or scheduleNodeId is required");
        }

        int effectiveLimit = normalizeLimit(limit);
        return schedulingActionRepository.searchByDefinitionAnchors(
                        normalizedWorkflowCode,
                        flowPlanVersionId,
                        scheduleNodeId,
                        PageRequest.of(0, effectiveLimit))
                .stream()
                .map(this::toSummaryView)
                .toList();
    }

    /**
     * Convert a persisted action into a complete audit view.
     *
     * @param action persisted action
     * @return complete action audit view
     */
    private ActionDetailView toDetailView(SchedulingAction action) {
        Optional<BackfillBatch> batch = findRelatedBackfillBatch(action);
        return new ActionDetailView(
                toSummaryView(action),
                action.getTargetAssetKey(),
                action.getTargetSnapshotId(),
                immutableMap(action.getRequestPayloadJson()),
                batch.map(this::toBatchEvidenceView).orElse(null),
                loadSnapshotEvidence(action, batch));
    }

    /**
     * Convert a persisted action into its list-safe summary view.
     *
     * @param action persisted action
     * @return action summary view
     */
    private ActionSummaryView toSummaryView(SchedulingAction action) {
        return new ActionSummaryView(
                action.getId(),
                action.getActionKey(),
                action.getActionType(),
                action.getScopeType(),
                action.getStatus(),
                action.getWorkflowCode(),
                action.getWorkflowVersion(),
                action.getFlowPlanVersionId(),
                action.getScheduleNodeId(),
                action.getWorkflowInstanceId(),
                action.getTaskInstanceId(),
                action.getProducedWorkflowInstanceId(),
                action.getProducedTaskInstanceId(),
                action.getBizDateStart(),
                action.getBizDateEnd(),
                action.getRequestedBy(),
                action.getReason(),
                action.getResultMessage(),
                action.getCreatedAt(),
                action.getUpdatedAt());
    }

    /**
     * Resolve a directly created or subsequently controlled backfill batch.
     *
     * @param action persisted scheduling action
     * @return related batch when this is a backfill action
     */
    private Optional<BackfillBatch> findRelatedBackfillBatch(SchedulingAction action) {
        if (!isBackfillAction(action)) {
            return Optional.empty();
        }

        Optional<BackfillBatch> createdBatch = backfillBatchRepository.findByActionKey(action.getActionKey());
        if (createdBatch.isPresent()) {
            return createdBatch;
        }
        return findBackfillBatchId(action.getRequestPayloadJson())
                .flatMap(backfillBatchRepository::findById);
    }

    /**
     * Check whether an action creates, recovers, or controls a backfill batch.
     *
     * @param action persisted scheduling action
     * @return true when a backfill batch may be related
     */
    private boolean isBackfillAction(SchedulingAction action) {
        return SCOPE_BACKFILL_BATCH.equals(action.getScopeType())
                || SchedulingActionTypes.BACKFILL_WORKFLOW.equals(action.getActionType())
                || SchedulingActionTypes.BACKFILL_NODE.equals(action.getActionType())
                || SchedulingActionTypes.RECOVER_BACKFILL.equals(action.getActionType());
    }

    /**
     * Read a positive backfill batch id from a structured request payload.
     *
     * @param payload action request payload
     * @return positive batch id when present and valid
     */
    private Optional<Long> findBackfillBatchId(Map<String, Object> payload) {
        if (payload == null) {
            return Optional.empty();
        }
        Object value = payload.get("backfillBatchId");
        if (value instanceof Number number) {
            long batchId = number.longValue();
            return batchId > 0 ? Optional.of(batchId) : Optional.empty();
        }
        if (value instanceof String text && !text.isBlank()) {
            try {
                long batchId = Long.parseLong(text.trim());
                return batchId > 0 ? Optional.of(batchId) : Optional.empty();
            } catch (NumberFormatException ignored) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    /**
     * Convert a persistent batch into action-linked audit evidence.
     *
     * @param batch persisted backfill batch
     * @return batch evidence view
     */
    private BackfillBatchEvidenceView toBatchEvidenceView(BackfillBatch batch) {
        return new BackfillBatchEvidenceView(
                batch.getId(),
                batch.getBatchKey(),
                batch.getActionKey(),
                batch.getScopeType(),
                immutableList(batch.getEntryNodeCodes()),
                immutableList(batch.getSelectedNodeCodes()),
                batch.getStartScheduleNodeId(),
                batch.getStartNodeCode(),
                batch.getBizDateStart(),
                batch.getBizDateEnd(),
                batch.getCascadePolicy(),
                batch.getProgressionMode(),
                batch.getMaxActiveDates(),
                batch.getSkipPolicy(),
                batch.getSkippedDateCount(),
                immutableMap(batch.getSkipEvidenceJson()),
                batch.getSourceBackfillBatchId(),
                batch.getRecoveryAttempt(),
                batch.getRecoveryStrategy(),
                batch.getStatus(),
                batch.getProducedWorkflowCount(),
                batch.getTotalItemCount());
    }

    /**
     * Load snapshot evidence through the strongest relation available.
     *
     * Backfill actions preserve batch item order. Other actions use the
     * affected workflow or task instance recorded on the action.
     *
     * @param action persisted action
     * @param batch related backfill batch when present
     * @return ordered target snapshot evidence
     */
    private List<TaskSnapshotEvidenceView> loadSnapshotEvidence(
            SchedulingAction action,
            Optional<BackfillBatch> batch) {

        if (batch.isPresent()) {
            return loadBackfillSnapshotEvidence(batch.get().getId());
        }
        if (action.getProducedWorkflowInstanceId() != null) {
            return taskInstanceRepository
                    .findByWorkflowInstanceIdOrderByCreatedAtAsc(action.getProducedWorkflowInstanceId())
                    .stream()
                    .map(task -> toTaskEvidenceView(task, null))
                    .toList();
        }

        Long taskInstanceId = action.getProducedTaskInstanceId() != null
                ? action.getProducedTaskInstanceId()
                : action.getTaskInstanceId();
        if (taskInstanceId == null) {
            return List.of();
        }
        return taskInstanceRepository.findById(taskInstanceId)
                .map(task -> List.of(toTaskEvidenceView(task, null)))
                .orElseGet(List::of);
    }

    /**
     * Load backfill task records in deterministic item order.
     *
     * Missing task records remain visible as incomplete audit evidence instead
     * of silently removing the corresponding batch item.
     *
     * @param backfillBatchId owning batch id
     * @return ordered batch task evidence
     */
    private List<TaskSnapshotEvidenceView> loadBackfillSnapshotEvidence(Long backfillBatchId) {
        List<BackfillItem> items = backfillItemRepository
                .findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(backfillBatchId);
        if (items.isEmpty()) {
            return List.of();
        }

        List<Long> taskIds = items.stream().map(BackfillItem::getTaskInstanceId).distinct().toList();
        Map<Long, TaskInstance> tasksById = taskInstanceRepository.findAllById(taskIds)
                .stream()
                .collect(Collectors.toMap(
                        TaskInstance::getId,
                        Function.identity(),
                        (left, right) -> left,
                        LinkedHashMap::new));

        return items.stream()
                .map(item -> {
                    TaskInstance task = tasksById.get(item.getTaskInstanceId());
                    return task == null ? toMissingTaskEvidenceView(item) : toTaskEvidenceView(task, item);
                })
                .toList();
    }

    /**
     * Convert a task and optional backfill item into snapshot evidence.
     *
     * @param task persisted task scheduling intent
     * @param item owning backfill item, or null
     * @return task snapshot evidence view
     */
    private TaskSnapshotEvidenceView toTaskEvidenceView(TaskInstance task, BackfillItem item) {
        IntentPublicationEvidence publication = findIntentPublicationEvidence(task.getId()).orElse(null);
        return new TaskSnapshotEvidenceView(
                item == null ? null : item.getId(),
                item == null ? null : item.getStatus(),
                true,
                task.getId(),
                task.getWorkflowInstanceId(),
                task.getFlowPlanVersionId(),
                task.getScheduleNodeId(),
                task.getTaskCode(),
                task.getBizDate(),
                task.getState(),
                task.getWaitingReason(),
                task.getTargetAssetKey(),
                task.getBaselineSnapshotId(),
                task.getObservedSnapshotId(),
                evaluateSnapshotProgress(task),
                task.getLastSnapshotCheckAt(),
                task.getScheduledAt(),
                publication == null ? null : publication.intentId(),
                publication == null ? null : publication.intentKey(),
                publication == null ? null : publication.channel(),
                publication == null ? null : publication.status(),
                publication == null ? null : publication.attemptCount(),
                publication == null ? null : publication.lastError(),
                publication == null ? null : publication.nextAttemptAt(),
                publication == null ? null : publication.deadLetteredAt(),
                publication == null ? null : publication.publishedAt(),
                task.getUpdatedAt());
    }

    /**
     * Join immutable intent and transport evidence without consulting downstream state.
     *
     * @param taskInstanceId task scheduling instance id
     * @return publication evidence when an outbox intent exists
     */
    private Optional<IntentPublicationEvidence> findIntentPublicationEvidence(Long taskInstanceId) {
        Optional<SchedulingIntent> intent = schedulingIntentRepository.findByTaskInstanceId(taskInstanceId);
        if (intent.isEmpty()) {
            return Optional.empty();
        }
        SchedulingIntentDelivery delivery = schedulingIntentDeliveryRepository
                .findBySchedulingIntentId(intent.get().getId())
                .orElse(null);
        return Optional.of(new IntentPublicationEvidence(
                intent.get().getId(),
                intent.get().getIntentKey(),
                delivery == null ? null : delivery.getChannel(),
                delivery == null ? null : delivery.getStatus(),
                delivery == null ? null : delivery.getAttemptCount(),
                delivery == null ? null : delivery.getLastError(),
                delivery == null ? null : delivery.getNextAttemptAt(),
                delivery == null ? null : delivery.getDeadLetteredAt(),
                delivery == null ? null : delivery.getPublishedAt()));
    }

    /**
     * Preserve a backfill item whose linked task record is unexpectedly absent.
     *
     * @param item persisted backfill item
     * @return incomplete task evidence view retaining batch anchors
     */
    private TaskSnapshotEvidenceView toMissingTaskEvidenceView(BackfillItem item) {
        return new TaskSnapshotEvidenceView(
                item.getId(),
                item.getStatus(),
                false,
                item.getTaskInstanceId(),
                item.getWorkflowInstanceId(),
                item.getFlowPlanVersionId(),
                item.getScheduleNodeId(),
                item.getNodeCode(),
                item.getBizDate().atStartOfDay(),
                null,
                "Linked task scheduling intent is missing",
                item.getTargetAssetKey(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                item.getUpdatedAt());
    }

    /**
     * Expose the result already derived by strict snapshot-intent attribution.
     *
     * Scheduler state is used only as the persisted result of snapshot evidence
     * evaluation. A newer but unattributed observed snapshot must remain null.
     *
     * @param task task carrying the snapshot-derived scheduling state
     * @return true when attributed evidence confirmed, false after expiry, otherwise null
     */
    private Boolean evaluateSnapshotProgress(TaskInstance task) {
        if (SchedulingStates.SNAPSHOT_CONFIRMED.equals(task.getState())) {
            return true;
        }
        if (SchedulingStates.SNAPSHOT_NOT_ADVANCED.equals(task.getState())) {
            return false;
        }
        return null;
    }

    /**
     * Normalize optional text filters.
     *
     * @param value source value
     * @return trimmed text or null
     */
    private String normalizeText(String value) {
        return isBlank(value) ? null : value.trim();
    }

    /**
     * Validate an optional positive persistence identifier.
     *
     * @param value identifier value
     * @param fieldName field name used in validation messages
     */
    private void validatePositiveId(Long value, String fieldName) {
        if (value != null && value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    /**
     * Apply the operational action-search result limit.
     *
     * @param limit requested limit, or null
     * @return positive limit capped at the service maximum
     */
    private int normalizeLimit(Integer limit) {
        if (limit == null) {
            return DEFAULT_LIMIT;
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return Math.min(limit, MAX_LIMIT);
    }

    /**
     * Copy a nullable list into an immutable read value.
     *
     * @param values source values
     * @return immutable values, or an empty list
     */
    private List<String> immutableList(List<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    /**
     * Copy a nullable map into an immutable read value.
     *
     * @param values source values
     * @return immutable values, or an empty map
     */
    private Map<String, Object> immutableMap(Map<String, Object> values) {
        return values == null ? Map.of() : Map.copyOf(values);
    }

    /**
     * Check whether text contains no meaningful value.
     *
     * @param value source value
     * @return true when null or blank
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
