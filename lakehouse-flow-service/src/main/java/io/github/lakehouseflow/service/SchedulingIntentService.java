package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.common.SchedulingIntentContract;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Commits scheduler-owned task intents and their selected delivery routes.
 *
 * Lakehouse Flow initiates publication after a task becomes ready. Downstream
 * systems never claim tasks from this service and never report execution
 * results back to it. A target snapshot baseline is frozen before the outbox
 * row becomes visible, and later snapshot progression remains the only result
 * evidence.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class SchedulingIntentService {

    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int MAX_BATCH_SIZE = 500;
    private static final String DATABASE_OUTBOX_DESTINATION = "scheduling_intent";
    private static final Set<String> SUPPORTED_DELIVERY_CHANNELS = Set.of(
            SchedulingIntentDeliveryChannels.DATABASE_TABLE,
            SchedulingIntentDeliveryChannels.HTTP,
            SchedulingIntentDeliveryChannels.MQ);

    private final TaskInstanceService taskInstanceService;
    private final WorkflowInstanceService workflowInstanceService;
    private final SnapshotProgressService snapshotProgressService;
    private final BackfillBatchRepository backfillBatchRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final FlowPlanVersionRepository flowPlanVersionRepository;
    private final SchedulingIntentRepository schedulingIntentRepository;
    private final SchedulingIntentDeliveryRepository schedulingIntentDeliveryRepository;
    private final SchedulingTargetAdmissionService schedulingTargetAdmissionService;
    private final FlowPlanPolicyService flowPlanPolicyService;
    private final InputSnapshotEvidenceService inputSnapshotEvidenceService;
    private final WriterJobBindingService writerJobBindingService;

    /** Global fallback for legacy tasks without a frozen FlowPlan policy. */
    @Value("${lakehouse-flow.snapshot-confirmation.timeout:PT1H}")
    private Duration targetAdmissionLease = Duration.ofHours(1);

    /** Selected outbound route for each immutable intent in this deployment. */
    @Value("${lakehouse-flow.scheduling-intent-delivery.channel:DATABASE_TABLE}")
    private String deliveryChannel = SchedulingIntentDeliveryChannels.DATABASE_TABLE;

    /** Channel-specific table, endpoint, or topic selected for publication. */
    @Value("${lakehouse-flow.scheduling-intent-delivery.destination:scheduling_intent}")
    private String deliveryDestination = DATABASE_OUTBOX_DESTINATION;

    /**
     * Signals that another scheduling decision currently owns a target-date slot.
     */
    private static final class SchedulingAdmissionConflictException extends IllegalStateException {

        /** Create one internal scan-skippable admission conflict. */
        private SchedulingAdmissionConflictException(String message) {
            super(message);
        }
    }

    /**
     * Task, workflow, and optional backfill evidence locked for one publication.
     *
     * @param task locked task scheduling decision
     * @param workflow locked owning workflow scheduling instance
     * @param flowPlanVersion locked immutable version for bounded concurrency, otherwise null
     * @param backfillItem locked owning backfill item, or null
     * @param effectivePolicy frozen runtime policy resolved before publication
     */
    private record LockedTaskIntentContext(
            TaskInstance task,
            WorkflowInstance workflow,
            FlowPlanVersion flowPlanVersion,
            BackfillItem backfillItem,
            FlowPlanPolicyService.EffectivePolicy effectivePolicy) {
    }

    /**
     * Read-only audit projection of an immutable scheduling intent and its route.
     *
     * @param intentId immutable intent id
     * @param contractVersion version of the downstream instruction contract
     * @param intentKey scheduler-generated idempotency key
     * @param taskInstanceId task scheduling decision id
     * @param workflowInstanceId owning workflow scheduling instance
     * @param triggerType scheduling trigger mode
     * @param backfillBatchId owning backfill batch id, or null
     * @param backfillItemId owning backfill item id, or null
     * @param taskCode task or node code delivered to the integration
     * @param taskVersion immutable task definition version
     * @param flowPlanVersionId immutable FlowPlanVersion anchor
     * @param scheduleNodeId immutable ScheduleNode anchor
     * @param bizDate business date represented by the instruction
     * @param targetAssetKey managed asset whose snapshot confirms the result
     * @param baselineSnapshotId snapshot frozen before publication
     * @param writerJobKey stable writer owning the target physical table
     * @param writerEpoch fenced writer generation for this data instruction
     * @param processingMode engine-neutral STREAMING or BATCH mode
     * @param inputSnapshotVector complete frozen parent and external input evidence
     * @param instructionPayload complete immutable downstream instruction
     * @param deliveryChannel transport channel used for publication
     * @param deliveryDestination channel-specific destination
     * @param deliveryStatus transport-only publication status
     * @param deliveryAttemptCount number of infrastructure publication attempts
     * @param deliveryLastError latest infrastructure error, or null
     * @param deliveryLastAttemptAt latest infrastructure attempt timestamp, or null
     * @param deliveryNextAttemptAt earliest next retry timestamp, or null
     * @param deliveryDeadLetteredAt terminal transport dead-letter timestamp, or null
     * @param publishedAt time the instruction became available through the channel
     * @param createdAt immutable intent creation timestamp
     */
    public record TaskSchedulingIntent(
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

    /**
     * Commit a bounded set of ready intents to the durable internal outbox.
     *
     * This method is called by an internal scheduler scanner. The limit controls
     * scheduler work per scan; it is not a downstream fetch or claim size.
     *
     * @param requestedLimit maximum intents to publish, or null for the default
     * @return immutable intents published by this scan
     */
    public List<TaskSchedulingIntent> publishReadyTaskIntents(Integer requestedLimit) {
        int limit = validateBatchSize(requestedLimit);
        List<TaskInstance> readyTasks = taskInstanceService.findSchedulableTasks().stream()
                .filter(task -> !isBlank(task.getTargetAssetKey()))
                .toList();
        if (readyTasks.isEmpty()) {
            return List.of();
        }

        Set<Long> unavailableTaskIds = backfillItemRepository.findUnavailableTaskInstanceIds(
                readyTasks.stream().map(TaskInstance::getId).toList(),
                BackfillItemStatuses.INTENT_READY,
                BackfillBatchStatuses.EXPANDED);
        List<TaskSchedulingIntent> published = new ArrayList<>(limit);
        for (TaskInstance task : readyTasks) {
            if (published.size() >= limit) {
                break;
            }
            if (unavailableTaskIds.contains(task.getId())) {
                continue;
            }
            try {
                published.add(publishTaskIntent(task.getId()));
            } catch (SchedulingAdmissionConflictException ignored) {
                // A later scanner retry can publish after the current holder releases or expires.
            }
        }
        return List.copyOf(published);
    }

    /**
     * Commit one ready scheduling decision and its selected delivery route.
     *
     * The target snapshot baseline is captured before publication. Repeating the
     * method for the same task returns the existing intent without changing that
     * baseline or consulting any downstream runtime state.
     *
     * @param taskInstanceId ready task scheduling instance id
     * @return newly published or existing immutable scheduling intent
     */
    public TaskSchedulingIntent publishTaskIntent(Long taskInstanceId) {
        LockedTaskIntentContext context = lockTaskIntentContext(taskInstanceId);
        Optional<SchedulingIntent> existing = schedulingIntentRepository.findByTaskInstanceId(taskInstanceId);
        if (existing.isPresent()) {
            return toIntent(existing.get());
        }

        TaskInstance task = context.task();
        if (SchedulingStates.CANCELLED.equals(context.workflow().getState())) {
            throw new IllegalStateException(
                    "Cancelled workflow does not allow intent publication: " + context.workflow().getId());
        }
        if (!SchedulingStates.READY_TO_SCHEDULE.equals(task.getState())) {
            throw new IllegalStateException(
                    "Only READY_TO_SCHEDULE task intents can be published: " + taskInstanceId);
        }
        requirePublishableBackfillItem(context.backfillItem());

        InputSnapshotEvidenceService.InputEvidenceEvaluation inputEvidence =
                inputSnapshotEvidenceService.evaluate(task);
        if (!inputEvidence.satisfied()) {
            throw new IllegalStateException(
                    "Scheduling intent input evidence is incomplete: " + inputEvidence.waitingReason());
        }
        List<Map<String, Object>> inputSnapshotVector = inputEvidence.evidence().stream()
                .map(io.github.lakehouseflow.model.InputSnapshotEvidence::toPayload)
                .toList();

        FlowPlanPolicyService.EffectivePolicy effectivePolicy = context.effectivePolicy();
        requireVersionConcurrencyAdmission(context, effectivePolicy);
        String selectedChannel = requireDeliveryChannel();
        String selectedDestination = requireDeliveryDestination();

        String targetAssetKey = requireTargetAsset(task);
        LocalDate bizDate = requireBizDate(task);
        String triggerType = requireTriggerType(context.workflow());
        String intentKey = "task-instance:" + task.getId();
        SchedulingTargetAdmissionService.AdmissionDecision admission =
                schedulingTargetAdmissionService.acquire(
                        targetAssetKey,
                        bizDate,
                        task.getId(),
                        intentKey,
                        triggerType,
                        effectivePolicy.targetAdmissionLease());
        if (!admission.admitted()) {
            throw new SchedulingAdmissionConflictException(
                    "Target and business date already admitted by task "
                            + admission.holderTaskInstanceId() + " until " + admission.expiresAt()
                            + ": " + targetAssetKey + "@" + bizDate);
        }
        WriterJobBindingService.WriterLease writerLease = writerJobBindingService.reserveDataIntent(
                task.getScheduleNodeId(),
                targetAssetKey,
                inputEvidence.processingMode(),
                intentKey,
                admission.expiresAt());
        if (!writerLease.admitted()) {
            schedulingTargetAdmissionService.release(
                    targetAssetKey,
                    bizDate,
                    task.getId(),
                    "WRITER_ADMISSION_REJECTED");
            throw new SchedulingAdmissionConflictException(writerLease.rejectionReason());
        }
        String baselineSnapshotId = snapshotProgressService.findLatestSnapshotId(targetAssetKey)
                .orElse(null);
        LocalDateTime now = LocalDateTime.now();
        Map<String, Object> instructionPayload = buildInstructionPayload(
                intentKey,
                task,
                context.workflow(),
                context.backfillItem(),
                targetAssetKey,
                baselineSnapshotId,
                writerLease.writerJobKey(),
                writerLease.writerEpoch(),
                inputEvidence.processingMode(),
                inputSnapshotVector,
                admission.expiresAt(),
                effectivePolicy,
                now);
        SchedulingIntent intent = schedulingIntentRepository.save(SchedulingIntent.builder()
                .contractVersion(SchedulingIntentContract.CONTRACT_VERSION)
                .intentKey(intentKey)
                .taskInstanceId(task.getId())
                .workflowInstanceId(task.getWorkflowInstanceId())
                .triggerType(triggerType)
                .backfillBatchId(context.backfillItem() == null
                        ? null
                        : context.backfillItem().getBackfillBatchId())
                .backfillItemId(context.backfillItem() == null
                        ? null
                        : context.backfillItem().getId())
                .flowPlanVersionId(task.getFlowPlanVersionId())
                .scheduleNodeId(task.getScheduleNodeId())
                .taskCode(task.getTaskCode())
                .taskVersion(task.getTaskVersion())
                .bizDate(task.getBizDate())
                .targetAssetKey(targetAssetKey)
                .baselineSnapshotId(baselineSnapshotId)
                .writerJobKey(writerLease.writerJobKey())
                .writerEpoch(writerLease.writerEpoch())
                .processingMode(inputEvidence.processingMode())
                .inputSnapshotVectorJson(inputSnapshotVector)
                .instructionPayloadJson(instructionPayload)
                .createdAt(now)
                .build());
        boolean databaseOutbox = SchedulingIntentDeliveryChannels.DATABASE_TABLE.equals(selectedChannel);
        SchedulingIntentDelivery delivery = schedulingIntentDeliveryRepository.save(
                SchedulingIntentDelivery.builder()
                        .schedulingIntentId(intent.getId())
                        .channel(selectedChannel)
                        .destination(selectedDestination)
                        .status(databaseOutbox
                                ? SchedulingIntentDeliveryStatuses.PUBLISHED
                                : SchedulingIntentDeliveryStatuses.PENDING)
                        .attemptCount(databaseOutbox ? 1 : 0)
                        .lastAttemptAt(databaseOutbox ? now : null)
                        .deliverBefore(admission.expiresAt())
                        .publishedAt(databaseOutbox ? now : null)
                        .createdAt(now)
                        .updatedAt(now)
                        .build());

        taskInstanceService.markScheduled(taskInstanceId, targetAssetKey, baselineSnapshotId);
        if (SchedulingStates.READY_TO_SCHEDULE.equals(context.workflow().getState())) {
            workflowInstanceService.markScheduled(context.workflow().getId());
        }
        markBackfillItemPublished(context.backfillItem());
        return toIntent(intent, delivery);
    }

    /**
     * Find the immutable published intent for a task instance.
     *
     * @param taskInstanceId task scheduling instance id
     * @return published intent and transport evidence when present
     */
    @Transactional(readOnly = true)
    public Optional<TaskSchedulingIntent> findTaskIntent(Long taskInstanceId) {
        if (taskInstanceId == null || taskInstanceId <= 0) {
            return Optional.empty();
        }
        return schedulingIntentRepository.findByTaskInstanceId(taskInstanceId)
                .map(this::toIntent);
    }

    /**
     * Lock scheduler records in batch, version, workflow, and task order for publication.
     *
     * @param taskInstanceId task scheduling instance id
     * @return locked publication context
     */
    private LockedTaskIntentContext lockTaskIntentContext(Long taskInstanceId) {
        if (taskInstanceId == null || taskInstanceId <= 0) {
            throw new IllegalArgumentException("taskInstanceId must be positive");
        }
        TaskInstance observedTask = taskInstanceRepository.findById(taskInstanceId)
                .orElseThrow(() -> new IllegalArgumentException("Task instance not found: " + taskInstanceId));
        FlowPlanPolicyService.EffectivePolicy effectivePolicy = flowPlanPolicyService.resolve(
                observedTask,
                targetAdmissionLease,
                targetAdmissionLease);
        BackfillItem backfillItem = SchedulingStates.READY_TO_SCHEDULE.equals(observedTask.getState())
                ? lockBackfillItemContext(taskInstanceId)
                : null;
        FlowPlanVersion flowPlanVersion = observedTask.getFlowPlanVersionId() == null
                || !SchedulingStates.READY_TO_SCHEDULE.equals(observedTask.getState())
                || effectivePolicy.maxActiveInstances() == Integer.MAX_VALUE
                ? null
                : flowPlanVersionRepository.findByIdForUpdate(observedTask.getFlowPlanVersionId())
                        .orElseThrow(() -> new IllegalStateException(
                                "FlowPlanVersion not found: " + observedTask.getFlowPlanVersionId()));
        WorkflowInstance workflow = workflowInstanceRepository.findByIdForUpdate(observedTask.getWorkflowInstanceId())
                .orElseThrow(() -> new IllegalStateException(
                        "Workflow instance not found: " + observedTask.getWorkflowInstanceId()));
        TaskInstance task = taskInstanceRepository.findByIdForUpdate(taskInstanceId)
                .orElseThrow(() -> new IllegalArgumentException("Task instance not found: " + taskInstanceId));
        return new LockedTaskIntentContext(task, workflow, flowPlanVersion, backfillItem, effectivePolicy);
    }

    /**
     * Admit the first intent of a workflow under its frozen version limit.
     *
     * A version row lock serializes this count-and-transition decision across
     * scheduler nodes. Tasks of a workflow already in SCHEDULED state keep
     * progressing through the DAG without consuming another instance slot.
     *
     * @param context locked publication context
     * @param policy effective immutable version policy
     */
    private void requireVersionConcurrencyAdmission(
            LockedTaskIntentContext context,
            FlowPlanPolicyService.EffectivePolicy policy) {
        if (context.flowPlanVersion() == null
                || !SchedulingStates.READY_TO_SCHEDULE.equals(context.workflow().getState())
                || policy.maxActiveInstances() == Integer.MAX_VALUE) {
            return;
        }
        long activeInstances = workflowInstanceRepository.countByFlowPlanVersionIdAndState(
                context.flowPlanVersion().getId(),
                SchedulingStates.SCHEDULED);
        if (activeInstances >= policy.maxActiveInstances()) {
            throw new SchedulingAdmissionConflictException(
                    "FlowPlanVersion active instance limit reached: "
                            + context.flowPlanVersion().getId() + " ("
                            + activeInstances + "/" + policy.maxActiveInstances() + ")");
        }
    }

    /**
     * Lock the owning backfill batch before workflow and task rows.
     *
     * @param taskInstanceId task scheduling instance id
     * @return owning item, or null for an ordinary scheduling intent
     */
    private BackfillItem lockBackfillItemContext(Long taskInstanceId) {
        Optional<Long> batchId = backfillItemRepository.findBackfillBatchIdByTaskInstanceId(taskInstanceId);
        if (batchId.isEmpty()) {
            return null;
        }

        BackfillBatch batch = backfillBatchRepository.findByIdForUpdate(batchId.get())
                .orElseThrow(() -> new IllegalStateException(
                        "Backfill batch not found: " + batchId.get()));
        if (!BackfillBatchStatuses.EXPANDED.equals(batch.getStatus())) {
            throw new IllegalStateException(
                    "Backfill batch does not allow intent publication in state " + batch.getStatus()
                            + ": " + batch.getId());
        }
        return backfillItemRepository.findByTaskInstanceId(taskInstanceId)
                .orElseThrow(() -> new IllegalStateException(
                        "Backfill item not found for task: " + taskInstanceId));
    }

    /**
     * Require an optional backfill item to be admitted for publication.
     *
     * @param backfillItem owning item, or null for an ordinary intent
     */
    private void requirePublishableBackfillItem(BackfillItem backfillItem) {
        if (backfillItem != null
                && !BackfillItemStatuses.INTENT_READY.equals(backfillItem.getStatus())) {
            throw new IllegalStateException(
                    "Backfill item is not ready for publication: " + backfillItem.getId());
        }
    }

    /**
     * Require the managed target asset used as the sole result boundary.
     *
     * @param task ready task scheduling decision
     * @return normalized target asset key
     */
    private String requireTargetAsset(TaskInstance task) {
        if (isBlank(task.getTargetAssetKey())) {
            throw new IllegalStateException(
                    "Scheduling intent requires a managed target asset: " + task.getId());
        }
        return task.getTargetAssetKey().trim();
    }

    /**
     * Require the business date used by persistent target admission.
     *
     * @param task ready task scheduling decision
     * @return date component of the immutable scheduling decision
     */
    private LocalDate requireBizDate(TaskInstance task) {
        if (task.getBizDate() == null) {
            throw new IllegalStateException(
                    "Scheduling intent requires a business date: " + task.getId());
        }
        return task.getBizDate().toLocalDate();
    }

    /**
     * Mark a backfill item as durably available in the database outbox.
     *
     * @param item owning backfill item, or null for an ordinary intent
     */
    private void markBackfillItemPublished(BackfillItem item) {
        if (item != null) {
            item.setStatus(BackfillItemStatuses.INTENT_DELIVERED);
            backfillItemRepository.save(item);
        }
    }

    /**
     * Convert an immutable intent and its delivery record into an audit view.
     *
     * @param intent persisted immutable intent
     * @param delivery persisted transport evidence
     * @return combined read model
     */
    private TaskSchedulingIntent toIntent(
            SchedulingIntent intent,
            SchedulingIntentDelivery delivery) {
        return new TaskSchedulingIntent(
                intent.getId(),
                intent.getContractVersion(),
                intent.getIntentKey(),
                intent.getTaskInstanceId(),
                intent.getWorkflowInstanceId(),
                intent.getTriggerType(),
                intent.getBackfillBatchId(),
                intent.getBackfillItemId(),
                intent.getTaskCode(),
                intent.getTaskVersion(),
                intent.getFlowPlanVersionId(),
                intent.getScheduleNodeId(),
                intent.getBizDate(),
                intent.getTargetAssetKey(),
                intent.getBaselineSnapshotId(),
                intent.getWriterJobKey(),
                intent.getWriterEpoch(),
                intent.getProcessingMode(),
                intent.getInputSnapshotVectorJson(),
                intent.getInstructionPayloadJson(),
                delivery.getChannel(),
                delivery.getDestination(),
                delivery.getStatus(),
                delivery.getAttemptCount(),
                delivery.getLastError(),
                delivery.getLastAttemptAt(),
                delivery.getNextAttemptAt(),
                delivery.getDeadLetteredAt(),
                delivery.getPublishedAt(),
                intent.getCreatedAt());
    }

    /**
     * Load transport evidence and convert an immutable intent into an audit view.
     *
     * @param intent persisted immutable intent
     * @return combined read model
     */
    private TaskSchedulingIntent toIntent(SchedulingIntent intent) {
        SchedulingIntentDelivery delivery = schedulingIntentDeliveryRepository
                .findBySchedulingIntentId(intent.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Scheduling intent delivery evidence not found: " + intent.getId()));
        return toIntent(intent, delivery);
    }

    /**
     * Validate and normalize one internal publication batch size.
     *
     * @param requestedLimit requested batch size, or null
     * @return effective batch size
     */
    private int validateBatchSize(Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_BATCH_SIZE : requestedLimit;
        if (limit <= 0 || limit > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "publication batch size must be between 1 and " + MAX_BATCH_SIZE);
        }
        return limit;
    }

    /**
     * Build the channel-neutral instruction and snapshot-attribution contract.
     *
     * @param intentKey immutable scheduling intent key
     * @param task task scheduling decision
     * @param workflow owning workflow scheduling decision
     * @param backfillItem owning backfill item, or null
     * @param targetAssetKey resolved target asset key
     * @param baselineSnapshotId snapshot frozen before publication, or null
     * @param writerJobKey stable writer owning the physical table
     * @param writerEpoch fenced writer generation
     * @param processingMode engine-neutral STREAMING or BATCH mode
     * @param inputSnapshotVector complete parent and external input evidence
     * @param admissionExpiresAt latest time at which downstream may start this intent
     * @param effectivePolicy frozen policy values used for scheduler decisions
     * @param issuedAt time at which the immutable instruction was created
     * @return JSON-compatible immutable instruction payload
     */
    private Map<String, Object> buildInstructionPayload(
            String intentKey,
            TaskInstance task,
            WorkflowInstance workflow,
            BackfillItem backfillItem,
            String targetAssetKey,
            String baselineSnapshotId,
            String writerJobKey,
            Long writerEpoch,
            String processingMode,
            List<Map<String, Object>> inputSnapshotVector,
            LocalDateTime admissionExpiresAt,
            FlowPlanPolicyService.EffectivePolicy effectivePolicy,
            LocalDateTime issuedAt) {
        Map<String, Object> identity = new LinkedHashMap<>();
        identity.put("taskInstanceId", task.getId());
        identity.put("workflowInstanceId", task.getWorkflowInstanceId());
        identity.put("flowPlanVersionId", task.getFlowPlanVersionId());
        identity.put("scheduleNodeId", task.getScheduleNodeId());

        Map<String, Object> definition = new LinkedHashMap<>();
        definition.put("workflowCode", workflow.getWorkflowCode());
        definition.put("workflowVersion", workflow.getWorkflowVersion());
        definition.put("taskCode", task.getTaskCode());
        definition.put("taskVersion", task.getTaskVersion());

        Map<String, Object> writer = new LinkedHashMap<>();
        writer.put("writerJobKey", writerJobKey);
        writer.put("writerEpoch", writerEpoch);
        writer.put("tableAssetKey", io.github.lakehouseflow.common.AssetKeys.tableKey(targetAssetKey)
                .orElseThrow(() -> new IllegalStateException("Invalid scheduling target table: " + targetAssetKey)));

        Map<String, Object> schedule = new LinkedHashMap<>();
        schedule.put("triggerType", requireTriggerType(workflow));
        schedule.put("triggerEventId", workflow.getTriggerEventId());
        schedule.put("triggerReason", workflow.getTriggerReason());
        schedule.put("bizDate", task.getBizDate().toLocalDate().toString());
        schedule.put("backfillBatchId", backfillItem == null ? null : backfillItem.getBackfillBatchId());
        schedule.put("backfillItemId", backfillItem == null ? null : backfillItem.getId());

        Map<String, String> requiredSnapshotProperties = new LinkedHashMap<>();
        requiredSnapshotProperties.put(
                SnapshotEvidenceContract.SOURCE_PROPERTY,
                SnapshotEvidenceContract.INTENT_SOURCE);
        requiredSnapshotProperties.put(
                SnapshotEvidenceContract.INTENT_KEY_PROPERTY,
                intentKey);
        requiredSnapshotProperties.put(
                SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY,
                writerJobKey);
        requiredSnapshotProperties.put(
                SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY,
                Long.toString(writerEpoch));
        requiredSnapshotProperties.put(
                SnapshotEvidenceContract.TARGET_ASSET_PROPERTY,
                targetAssetKey);
        requiredSnapshotProperties.put(
                SnapshotEvidenceContract.BIZ_DATE_PROPERTY,
                task.getBizDate().toLocalDate().toString());
        requiredSnapshotProperties.put(
                SnapshotEvidenceContract.FINAL_PROPERTY,
                SnapshotEvidenceContract.FINAL_VALUE);

        Map<String, Object> snapshotEvidence = new LinkedHashMap<>();
        snapshotEvidence.put("mode", SnapshotEvidenceContract.CONFIRMATION_MODE);
        snapshotEvidence.put("targetAssetKey", targetAssetKey);
        snapshotEvidence.put("baselineSnapshotId", baselineSnapshotId);
        snapshotEvidence.put("confirmationTimeout", effectivePolicy.confirmationTimeout().toString());
        snapshotEvidence.put("expectedPartition", extractPartition(targetAssetKey));
        snapshotEvidence.put("requiredSnapshotChangeType", SnapshotEvidenceContract.REQUIRED_CHANGE_TYPE);
        snapshotEvidence.put("requiredSnapshotProperties", requiredSnapshotProperties);

        Map<String, Object> publicationAdmission = new LinkedHashMap<>();
        publicationAdmission.put("targetAssetKey", targetAssetKey);
        publicationAdmission.put("bizDate", task.getBizDate().toLocalDate().toString());
        publicationAdmission.put("holderIntentKey", intentKey);
        publicationAdmission.put("leaseExpiresAt", admissionExpiresAt.toString());

        Map<String, Object> schedulingPolicy = new LinkedHashMap<>();
        schedulingPolicy.put("concurrencyMode", effectivePolicy.concurrencyMode());
        schedulingPolicy.put("maxActiveInstances",
                effectivePolicy.maxActiveInstances() == Integer.MAX_VALUE
                        ? null
                        : effectivePolicy.maxActiveInstances());

        Map<String, Object> processing = new LinkedHashMap<>();
        processing.put("processingMode", processingMode);
        processing.put("inputSnapshotVector", inputSnapshotVector);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", SchedulingIntentContract.CONTRACT_VERSION);
        payload.put("source", SnapshotEvidenceContract.INTENT_SOURCE);
        payload.put("intentKind", SchedulingIntentContract.INTENT_KIND);
        payload.put("intentKey", intentKey);
        payload.put("issuedAt", issuedAt.toString());
        payload.put("identity", identity);
        payload.put("definition", definition);
        payload.put("writer", writer);
        payload.put("schedule", schedule);
        payload.put("processing", processing);
        payload.put("schedulingPolicy", schedulingPolicy);
        payload.put("publicationAdmission", publicationAdmission);
        payload.put("snapshotEvidence", snapshotEvidence);
        return payload;
    }

    /**
     * Require the workflow trigger type copied into the immutable instruction.
     *
     * @param workflow owning workflow scheduling decision
     * @return normalized trigger type
     */
    private String requireTriggerType(WorkflowInstance workflow) {
        if (workflow == null || isBlank(workflow.getTriggerType())) {
            throw new IllegalStateException("Scheduling intent requires a workflow trigger type");
        }
        return workflow.getTriggerType().trim();
    }

    /** Resolve and validate the single selected delivery channel. */
    private String requireDeliveryChannel() {
        if (isBlank(deliveryChannel)) {
            throw new IllegalStateException("Scheduling intent delivery channel must not be blank");
        }
        String normalized = deliveryChannel.trim().toUpperCase(java.util.Locale.ROOT);
        if (!SUPPORTED_DELIVERY_CHANNELS.contains(normalized)) {
            throw new IllegalStateException("Unsupported scheduling intent delivery channel: " + deliveryChannel);
        }
        return normalized;
    }

    /** Resolve and validate the destination for the selected delivery route. */
    private String requireDeliveryDestination() {
        if (isBlank(deliveryDestination)) {
            throw new IllegalStateException("Scheduling intent delivery destination must not be blank");
        }
        return deliveryDestination.trim();
    }

    /**
     * Extract the optional partition suffix from an asset key.
     *
     * Asset keys use catalog.database.table[.partition], and the split limit
     * preserves dots that may occur inside a partition value.
     *
     * @param targetAssetKey resolved target asset key
     * @return partition suffix, or null for a table-level target
     */
    private String extractPartition(String targetAssetKey) {
        String[] parts = targetAssetKey.split("\\.", 4);
        return parts.length == 4 ? parts[3] : null;
    }

    /**
     * Check whether text is null or blank.
     *
     * @param value source value
     * @return true when no meaningful text is present
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
