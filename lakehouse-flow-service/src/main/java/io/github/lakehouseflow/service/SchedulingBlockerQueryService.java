package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.AssetKeys;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.OperationalBlockerTypes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.common.SnapshotSourceHealthOutcomes;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Unified read-only query for current scheduler, delivery, and snapshot-evidence blockers.
 *
 * <p>The aggregation deliberately excludes downstream runtime state. Every row is derived from
 * Lakehouse Flow's own scheduling state, transport evidence, or source/snapshot evidence.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SchedulingBlockerQueryService {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 500;
    private static final String TASK_INSTANCE = "TASK_INSTANCE";
    private static final String BACKFILL_ITEM = "BACKFILL_ITEM";
    private static final String JOB_CONTROL_INTENT = "JOB_CONTROL_INTENT";
    private static final String SCHEDULING_INTENT_DELIVERY = "SCHEDULING_INTENT_DELIVERY";
    private static final String JOB_CONTROL_INTENT_DELIVERY = "JOB_CONTROL_INTENT_DELIVERY";

    private final TaskInstanceRepository taskInstanceRepository;
    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final BackfillBatchRepository backfillBatchRepository;
    private final JobControlIntentRepository jobControlIntentRepository;
    private final SchedulingIntentDeliveryQueryService schedulingDeliveryQueryService;
    private final JobControlIntentDeliveryQueryService jobControlDeliveryQueryService;

    /**
     * Find current blockers across scheduler-owned records using stable bounded categories.
     *
     * @param blockerType optional blocker category
     * @param flowCode optional owning or affected Flow code
     * @param targetAssetKey optional exact target or physical-table prefix
     * @param requestedLimit maximum rows, or null for the default
     * @return newest matching blockers first
     */
    public List<OperationalBlocker> findBlockers(
            String blockerType,
            String flowCode,
            String targetAssetKey,
            Integer requestedLimit) {
        String normalizedType = OperationalBlockerTypes.normalizeOptional(blockerType);
        String normalizedFlowCode = normalizeText(flowCode);
        String normalizedTarget = normalizeText(targetAssetKey);
        int limit = validateLimit(requestedLimit);
        PageRequest page = PageRequest.of(0, limit);
        List<OperationalBlocker> blockers = new ArrayList<>();

        if (includesTaskBlockers(normalizedType)) {
            blockers.addAll(loadTaskBlockers(normalizedType, normalizedFlowCode, normalizedTarget, page));
        }
        if (includesBackfillBlockers(normalizedType)) {
            blockers.addAll(loadBackfillBlockers(normalizedType, normalizedFlowCode, normalizedTarget, page));
        }
        if (includesJobControlBlockers(normalizedType)) {
            blockers.addAll(loadJobControlBlockers(normalizedType, normalizedFlowCode, normalizedTarget, page));
        }
        if (normalizedType == null || OperationalBlockerTypes.DELIVERY_EXHAUSTED.equals(normalizedType)) {
            blockers.addAll(loadDeliveryBlockers(normalizedFlowCode, normalizedTarget, limit));
        }

        return blockers.stream()
                .sorted(Comparator.comparing(
                                OperationalBlocker::observedAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(
                                OperationalBlocker::subjectId,
                                Comparator.nullsLast(Comparator.reverseOrder())))
                .limit(limit)
                .toList();
    }

    /** Load task wait phases and attach their owning workflow identity. */
    private List<OperationalBlocker> loadTaskBlockers(
            String blockerType,
            String flowCode,
            String targetAssetKey,
            PageRequest page) {
        List<TaskInstance> tasks = taskInstanceRepository.findOperationalBlockers(
                blockerType, flowCode, targetAssetKey, page);
        Map<Long, WorkflowInstance> workflows = workflowInstanceRepository.findAllById(tasks.stream()
                        .map(TaskInstance::getWorkflowInstanceId)
                        .distinct()
                        .toList())
                .stream()
                .collect(Collectors.toMap(WorkflowInstance::getId, Function.identity()));
        return tasks.stream()
                .map(task -> toTaskBlocker(task, workflows.get(task.getWorkflowInstanceId())))
                .toList();
    }

    /** Convert one current task wait phase into a stable blocker category. */
    private OperationalBlocker toTaskBlocker(TaskInstance task, WorkflowInstance workflow) {
        String blockerType;
        if (SchedulingStates.WAITING_SNAPSHOT.equals(task.getState())) {
            blockerType = OperationalBlockerTypes.INPUT_SNAPSHOT;
        } else if (SchedulingStates.READY_TO_SCHEDULE.equals(task.getState())) {
            blockerType = OperationalBlockerTypes.INTENT_PUBLICATION;
        } else if (isSourceBlocked(task.getSourceHealth())) {
            blockerType = OperationalBlockerTypes.SOURCE_BLOCKED;
        } else {
            blockerType = OperationalBlockerTypes.TARGET_SNAPSHOT;
        }
        return new OperationalBlocker(
                blockerType,
                TASK_INSTANCE,
                task.getId(),
                task.getInstanceKey(),
                workflow == null ? null : workflow.getWorkflowCode(),
                task.getTaskCode(),
                null,
                task.getTargetAssetKey(),
                task.getBizDate(),
                task.getState(),
                null,
                task.getSourceHealth(),
                defaultReason(task.getWaitingReason(), blockerType),
                task.getUpdatedAt());
    }

    /** Load blocked backfill items and attach their immutable batch identity. */
    private List<OperationalBlocker> loadBackfillBlockers(
            String blockerType,
            String flowCode,
            String targetAssetKey,
            PageRequest page) {
        List<BackfillItem> items = backfillItemRepository.findOperationalBlockers(
                blockerType, flowCode, targetAssetKey, page);
        Map<Long, BackfillBatch> batches = backfillBatchRepository.findAllById(items.stream()
                        .map(BackfillItem::getBackfillBatchId)
                        .distinct()
                        .toList())
                .stream()
                .collect(Collectors.toMap(BackfillBatch::getId, Function.identity()));
        return items.stream()
                .map(item -> toBackfillBlocker(item, batches.get(item.getBackfillBatchId())))
                .toList();
    }

    /** Convert one backfill admission status into its stable blocker category. */
    private OperationalBlocker toBackfillBlocker(BackfillItem item, BackfillBatch batch) {
        String blockerType = BackfillItemStatuses.WAITING_CONCURRENCY.equals(item.getStatus())
                ? OperationalBlockerTypes.DATE_CONCURRENCY
                : OperationalBlockerTypes.DAG_DEPENDENCY;
        String subjectKey = batch == null
                ? Long.toString(item.getId())
                : batch.getBatchKey() + ":" + item.getNodeCode() + ":" + item.getBizDate();
        return new OperationalBlocker(
                blockerType,
                BACKFILL_ITEM,
                item.getId(),
                subjectKey,
                batch == null ? null : batch.getWorkflowCode(),
                item.getNodeCode(),
                null,
                item.getTargetAssetKey(),
                item.getBizDate().atStartOfDay(),
                item.getStatus(),
                null,
                null,
                OperationalBlockerTypes.DATE_CONCURRENCY.equals(blockerType)
                        ? "Backfill business date is waiting for an available progression slot"
                        : "Backfill node is waiting for upstream snapshot confirmation",
                item.getUpdatedAt());
    }

    /** Load platform writer lifecycle intents waiting for snapshot or source evidence. */
    private List<OperationalBlocker> loadJobControlBlockers(
            String blockerType,
            String flowCode,
            String targetAssetKey,
            PageRequest page) {
        String tableAssetKey = normalizeTableAssetKey(targetAssetKey);
        return jobControlIntentRepository.findOperationalBlockers(
                        blockerType, flowCode, tableAssetKey, page)
                .stream()
                .map(intent -> toJobControlBlocker(intent, flowCode))
                .toList();
    }

    /** Convert one writer lifecycle wait into a target-snapshot or source blocker. */
    private OperationalBlocker toJobControlBlocker(JobControlIntent intent, String flowCode) {
        String blockerType = isSourceBlocked(intent.getSourceHealth())
                ? OperationalBlockerTypes.SOURCE_BLOCKED
                : OperationalBlockerTypes.TARGET_SNAPSHOT;
        LocalDateTime observedAt = intent.getLastSnapshotCheckAt() == null
                ? intent.getCreatedAt()
                : intent.getLastSnapshotCheckAt();
        return new OperationalBlocker(
                blockerType,
                JOB_CONTROL_INTENT,
                intent.getId(),
                intent.getIntentKey(),
                flowCode,
                null,
                intent.getWriterJobKey(),
                intent.getTableAssetKey(),
                null,
                intent.getSnapshotResult(),
                null,
                intent.getSourceHealth(),
                defaultReason(intent.getWaitingReason(), blockerType),
                observedAt);
    }

    /** Load both transport dead-letter families and keep them independent from snapshot results. */
    private List<OperationalBlocker> loadDeliveryBlockers(
            String flowCode,
            String targetAssetKey,
            int limit) {
        List<OperationalBlocker> blockers = new ArrayList<>();
        schedulingDeliveryQueryService.findDeadLetters(null, flowCode, targetAssetKey, limit).stream()
                .map(this::toSchedulingDeliveryBlocker)
                .forEach(blockers::add);

        jobControlDeliveryQueryService.findDeadLetters(null, flowCode, targetAssetKey, limit).stream()
                .map(deadLetter -> toJobControlDeliveryBlocker(deadLetter, flowCode))
                .forEach(blockers::add);
        return blockers;
    }

    /** Convert one exhausted data scheduling-intent transport into a blocker row. */
    private OperationalBlocker toSchedulingDeliveryBlocker(
            SchedulingIntentDeliveryQueryService.DeadLetterDelivery deadLetter) {
        return new OperationalBlocker(
                OperationalBlockerTypes.DELIVERY_EXHAUSTED,
                SCHEDULING_INTENT_DELIVERY,
                deadLetter.deliveryId(),
                deadLetter.intentKey(),
                deadLetter.flowCode(),
                deadLetter.taskCode(),
                null,
                deadLetter.targetAssetKey(),
                deadLetter.bizDate(),
                null,
                "EXHAUSTED",
                null,
                deadLetter.lastError(),
                deadLetter.deadLetteredAt());
    }

    /** Convert one exhausted writer lifecycle transport into a blocker row. */
    private OperationalBlocker toJobControlDeliveryBlocker(
            JobControlIntentDeliveryQueryService.DeadLetterDelivery deadLetter,
            String flowCode) {
        return new OperationalBlocker(
                OperationalBlockerTypes.DELIVERY_EXHAUSTED,
                JOB_CONTROL_INTENT_DELIVERY,
                deadLetter.deliveryId(),
                deadLetter.intentKey(),
                flowCode,
                null,
                deadLetter.writerJobKey(),
                deadLetter.tableAssetKey(),
                null,
                null,
                "EXHAUSTED",
                null,
                deadLetter.lastError(),
                deadLetter.deadLetteredAt());
    }

    /** Determine whether the selected category can be represented by a task wait phase. */
    private boolean includesTaskBlockers(String blockerType) {
        return blockerType == null
                || OperationalBlockerTypes.INPUT_SNAPSHOT.equals(blockerType)
                || OperationalBlockerTypes.INTENT_PUBLICATION.equals(blockerType)
                || OperationalBlockerTypes.TARGET_SNAPSHOT.equals(blockerType)
                || OperationalBlockerTypes.SOURCE_BLOCKED.equals(blockerType);
    }

    /** Determine whether the selected category can be represented by a backfill item. */
    private boolean includesBackfillBlockers(String blockerType) {
        return blockerType == null
                || OperationalBlockerTypes.DAG_DEPENDENCY.equals(blockerType)
                || OperationalBlockerTypes.DATE_CONCURRENCY.equals(blockerType);
    }

    /** Determine whether the selected category can be represented by a job-control intent. */
    private boolean includesJobControlBlockers(String blockerType) {
        return blockerType == null
                || OperationalBlockerTypes.TARGET_SNAPSHOT.equals(blockerType)
                || OperationalBlockerTypes.SOURCE_BLOCKED.equals(blockerType);
    }

    /** Treat repairable and unavailable source evidence as independent source blockers. */
    private boolean isSourceBlocked(String sourceHealth) {
        return SnapshotSourceHealthOutcomes.REPAIRABLE.equals(sourceHealth)
                || SnapshotSourceHealthOutcomes.SOURCE_BLOCKED.equals(sourceHealth);
    }

    /** Normalize an optional table/partition key for platform writer queries. */
    private String normalizeTableAssetKey(String targetAssetKey) {
        if (targetAssetKey == null) {
            return null;
        }
        return AssetKeys.tableKey(targetAssetKey)
                .orElseThrow(() -> new IllegalArgumentException(
                        "targetAssetKey must be catalog.database.table[.partition]"));
    }

    /** Use a stable category when no more specific human-readable reason was persisted. */
    private String defaultReason(String reason, String blockerType) {
        return reason == null || reason.isBlank() ? blockerType : reason;
    }

    /** Normalize optional free text by trimming it. */
    private String normalizeText(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    /** Validate a bounded operations query size. */
    private int validateLimit(Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_LIMIT : requestedLimit;
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("blocker query limit must be between 1 and " + MAX_LIMIT);
        }
        return limit;
    }

    /**
     * One current scheduler, delivery, or source/snapshot blocker.
     *
     * @param blockerType stable blocker category
     * @param subjectType scheduler-owned record family
     * @param subjectId scheduler-owned record id
     * @param subjectKey stable task, batch-item, or intent identity
     * @param flowCode owning or affected Flow when known
     * @param taskCode task or node code when applicable
     * @param writerJobKey platform writer key when applicable
     * @param targetAssetKey target asset related to the blocker
     * @param bizDate business date when applicable
     * @param schedulingState scheduler-side state when applicable
     * @param deliveryStatus transport-only state when applicable
     * @param sourceHealth independent source-health result when applicable
     * @param reason scheduler, transport, or source evidence detail
     * @param observedAt latest evidence update time
     */
    public record OperationalBlocker(
            String blockerType,
            String subjectType,
            Long subjectId,
            String subjectKey,
            String flowCode,
            String taskCode,
            String writerJobKey,
            String targetAssetKey,
            LocalDateTime bizDate,
            String schedulingState,
            String deliveryStatus,
            String sourceHealth,
            String reason,
            LocalDateTime observedAt) {
    }
}
