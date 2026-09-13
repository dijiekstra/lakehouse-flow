package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillCascadePolicies;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.common.BackfillRecoveryStrategies;
import io.github.lakehouseflow.common.BackfillScopeTypes;
import io.github.lakehouseflow.common.BackfillSkipPolicies;
import io.github.lakehouseflow.common.FlowPlanVersionStatuses;
import io.github.lakehouseflow.common.SchedulingActionStatuses;
import io.github.lakehouseflow.common.SchedulingActionTypes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.SchedulingActionRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.SchedulingAction;
import io.github.lakehouseflow.model.SchedulingActionResult;
import io.github.lakehouseflow.model.SnapshotConfirmationResult;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applies operator/system actions to Lakehouse Flow scheduling records.
 *
 * This service intentionally stays on the scheduler side: rerun and backfill
 * emit new workflow scheduling decisions, while cancel/skip/recheck update
 * scheduling state or snapshot evidence only. No external task execution is
 * started here.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class SchedulingActionService {

    private static final String SCOPE_WORKFLOW_INSTANCE = "WORKFLOW_INSTANCE";
    private static final String SCOPE_TASK_INSTANCE = "TASK_INSTANCE";
    private static final String SCOPE_WORKFLOW_DEFINITION = "WORKFLOW_DEFINITION";
    private static final String SCOPE_SCHEDULE_NODE = "SCHEDULE_NODE";
    private static final String SCOPE_BACKFILL_BATCH = "BACKFILL_BATCH";
    private final SchedulingActionRepository schedulingActionRepository;
    private final BackfillBatchRepository backfillBatchRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final FlowPlanVersionRepository flowPlanVersionRepository;
    private final ScheduleNodeRepository scheduleNodeRepository;
    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final WorkflowInstanceService workflowInstanceService;
    private final TaskInstanceService taskInstanceService;
    private final SnapshotConfirmationService snapshotConfirmationService;
    private final BackfillProgressionService backfillProgressionService;
    private final FlowPlanGraphService flowPlanGraphService;
    private final SchedulingTemplateResolver schedulingTemplateResolver;

    /**
     * Pair used while validating all backfill items before applying cancellation.
     *
     * @param item persistent backfill item
     * @param task generated task scheduling intent
     */
    private record BackfillItemTask(BackfillItem item, TaskInstance task) {
    }

    /**
     * Validated date-admission policy for one node backfill request.
     *
     * @param mode normalized progression mode
     * @param maxActiveDates finite active-date limit, or null for unrestricted parallel progression
     */
    private record BackfillProgressionPolicy(String mode, Integer maxActiveDates) {
    }

    /**
     * Identifiers produced while one backfill batch is expanded into scheduling intents.
     *
     * @param workflowInstanceIds generated date-scoped workflow wrapper ids
     * @param taskInstanceIds generated node/date scheduling intent ids
     * @param skipEvidence date-keyed historical task evidence for whole-date skips
     */
    private record BackfillExpansion(
            List<Long> workflowInstanceIds,
            List<Long> taskInstanceIds,
            Map<String, Object> skipEvidence) {
    }

    /**
     * Immutable graph scope selected for one replacement backfill batch.
     *
     * @param scopeType complete-flow or node-subgraph batch scope
     * @param startNode primary node-subgraph recovery entry, or null for complete-flow recovery
     * @param targetNodes nodes regenerated for every recovered business date
     * @param entryNodeCodes nodes explicitly allowed to bypass omitted parents
     * @param cascadePolicy node-subgraph cascade policy, or null for complete-flow recovery
     * @param failedNodeCodes snapshot-failed node codes that justified recovery
     */
    private record BackfillRecoveryScope(
            String scopeType,
            ScheduleNode startNode,
            List<ScheduleNode> targetNodes,
            Set<String> entryNodeCodes,
            String cascadePolicy,
            List<String> failedNodeCodes) {
    }

    /**
     * Rerun a workflow instance by emitting a new workflow scheduling decision.
     *
     * @param workflowInstanceId existing workflow instance to rerun
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult rerunWorkflowInstance(
            Long workflowInstanceId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = saveAcceptedAction(SchedulingAction.builder()
                .actionKey(requireActionKey(actionKey))
                .actionType(SchedulingActionTypes.RERUN_WORKFLOW)
                .scopeType(SCOPE_WORKFLOW_INSTANCE)
                .workflowInstanceId(workflowInstanceId)
                .requestedBy(requestedBy)
                .reason(reason)
                .requestPayloadJson(payload("sourceWorkflowInstanceId", workflowInstanceId))
                .build());

        try {
            WorkflowInstance source = workflowInstanceRepository.findById(workflowInstanceId)
                    .orElseThrow(() -> new RuntimeException("Workflow instance not found: " + workflowInstanceId));
            copyWorkflowScope(action, source);
            WorkflowInstance rerun = workflowInstanceService.createInstance(
                    source.getWorkflowCode(),
                    source.getWorkflowVersion(),
                    source.getBizDate(),
                    SchedulingActionTypes.toTriggerType(SchedulingActionTypes.RERUN_WORKFLOW),
                    actionKey,
                    actionReason("Rerun workflow instance " + workflowInstanceId, reason),
                    source.getFlowPlanVersionId());
            List<TaskInstance> emittedTasks = emitWorkflowRerunTasks(source, rerun);
            markWorkflowDecisionEmitted(rerun);

            return markApplied(
                    action,
                    List.of(rerun.getId()),
                    emittedTasks.isEmpty() ? null : emittedTasks.get(0).getId(),
                    "Rerun scheduling intents emitted for workflow instance " + workflowInstanceId
                            + ": taskIntents=" + emittedTasks.size());
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Rerun a task scheduling instance by emitting a fresh task scheduling intent.
     *
     * The new intent is represented as a task instance under a new action-owned
     * workflow wrapper. Lakehouse Flow still does not execute the task; downstream
     * systems must consume the new READY_TO_SCHEDULE intent and snapshot progress
     * remains the confirmation source of truth.
     *
     * @param taskInstanceId existing task instance to rerun
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult rerunTaskInstance(
            Long taskInstanceId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startSimpleAction(
                SchedulingActionTypes.RERUN_TASK,
                SCOPE_TASK_INSTANCE,
                actionKey,
                null,
                taskInstanceId,
                requestedBy,
                reason);

        try {
            TaskInstance sourceTask = taskInstanceRepository.findById(taskInstanceId)
                    .orElseThrow(() -> new RuntimeException("Task instance not found: " + taskInstanceId));
            WorkflowInstance sourceWorkflow = workflowInstanceRepository.findById(sourceTask.getWorkflowInstanceId())
                    .orElseThrow(() -> new RuntimeException(
                            "Workflow instance not found: " + sourceTask.getWorkflowInstanceId()));
            copyTaskScope(action, sourceTask, sourceWorkflow);

            LocalDateTime effectiveBizDate = sourceTask.getBizDate() != null
                    ? sourceTask.getBizDate()
                    : sourceWorkflow.getBizDate();
            WorkflowInstance rerunWorkflow = workflowInstanceService.createInstance(
                    sourceWorkflow.getWorkflowCode(),
                    sourceWorkflow.getWorkflowVersion(),
                    effectiveBizDate,
                    SchedulingActionTypes.toTriggerType(SchedulingActionTypes.RERUN_TASK),
                    actionKey,
                    actionReason("Rerun task instance " + taskInstanceId, reason),
                    sourceTask.getFlowPlanVersionId());

            TaskInstance rerunTask = taskInstanceService.createInstance(
                    rerunWorkflow.getId(),
                    sourceTask.getTaskCode(),
                    sourceTask.getTaskVersion(),
                    effectiveBizDate,
                    sourceTask.getTargetAssetKey(),
                    sourceTask.getFlowPlanVersionId(),
                    sourceTask.getScheduleNodeId());
            taskInstanceService.markSchedulable(rerunTask.getId());
            markWorkflowDecisionEmitted(rerunWorkflow);

            return markApplied(
                    action,
                    List.of(rerunWorkflow.getId()),
                    rerunTask.getId(),
                    "Task rerun scheduling intent emitted for task instance " + taskInstanceId);
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Rerun one published FlowPlan node by emitting a new task scheduling intent.
     *
     * The node definition provides the task code and target asset. Lakehouse Flow
     * emits a scheduling intent only; downstream consumption and later snapshot
     * progression remain separate steps.
     *
     * @param flowPlanVersionId published FlowPlanVersion id
     * @param nodeCode ScheduleNode code inside the version
     * @param bizDate business date for the rerun intent
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult rerunScheduleNode(
            Long flowPlanVersionId,
            String nodeCode,
            LocalDate bizDate,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = saveAcceptedAction(SchedulingAction.builder()
                .actionKey(requireActionKey(actionKey))
                .actionType(SchedulingActionTypes.RERUN_TASK)
                .scopeType(SCOPE_SCHEDULE_NODE)
                .flowPlanVersionId(flowPlanVersionId)
                .bizDateStart(bizDate)
                .bizDateEnd(bizDate)
                .requestedBy(requestedBy)
                .reason(reason)
                .requestPayloadJson(payload(
                        "flowPlanVersionId", flowPlanVersionId,
                        "nodeCode", nodeCode,
                        "bizDate", String.valueOf(bizDate)))
                .build());

        try {
            Long versionId = requirePositiveId(flowPlanVersionId, "flowPlanVersionId");
            String normalizedNodeCode = requireText(nodeCode, "nodeCode");
            LocalDate normalizedBizDate = requireBizDate(bizDate);
            FlowPlanVersion version = flowPlanVersionRepository.findById(versionId)
                    .orElseThrow(() -> new RuntimeException("FlowPlanVersion not found: " + versionId));
            if (!FlowPlanVersionStatuses.isPublished(version.getStatus())) {
                throw new IllegalStateException("Only published FlowPlanVersion can be rerun: " + versionId);
            }
            List<ScheduleNode> versionNodes = flowPlanGraphService.validateAndOrder(scheduleNodeRepository
                    .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(versionId));
            ScheduleNode node = flowPlanGraphService.findNode(versionNodes, normalizedNodeCode);
            String targetAssetKey = schedulingTemplateResolver.resolve(node.getOutputAssetKey(), normalizedBizDate);
            action.setWorkflowCode(version.getFlowCode());
            action.setWorkflowVersion(version.getVersion());
            action.setScheduleNodeId(node.getId());
            action.setTargetAssetKey(targetAssetKey);

            LocalDateTime effectiveBizDate = normalizedBizDate.atStartOfDay();
            WorkflowInstance rerunWorkflow = workflowInstanceService.createInstance(
                    version.getFlowCode(),
                    version.getVersion(),
                    effectiveBizDate,
                    SchedulingActionTypes.toTriggerType(SchedulingActionTypes.RERUN_TASK),
                    actionKey,
                    actionReason("Rerun schedule node " + normalizedNodeCode + " in version " + versionId, reason),
                    versionId);
            TaskInstance rerunTask = taskInstanceService.createInstance(
                    rerunWorkflow.getId(),
                    node.getNodeCode(),
                    version.getVersion(),
                    effectiveBizDate,
                    targetAssetKey,
                    versionId,
                    node.getId());
            taskInstanceService.markSchedulable(rerunTask.getId());
            markWorkflowDecisionEmitted(rerunWorkflow);

            return markApplied(
                    action,
                    List.of(rerunWorkflow.getId()),
                    rerunTask.getId(),
                    "Node rerun scheduling intent emitted for " + normalizedNodeCode);
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Create backfill workflow scheduling decisions for an inclusive date range.
     *
     * @param workflowCode workflow definition code
     * @param workflowVersion workflow definition version; defaults to 1 when null
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult backfillWorkflow(
            String workflowCode,
            Integer workflowVersion,
            LocalDate startBizDate,
            LocalDate endBizDate,
            String actionKey,
            String requestedBy,
            String reason) {

        return backfillWorkflow(
                workflowCode,
                workflowVersion,
                startBizDate,
                endBizDate,
                BackfillProgressionModes.PARALLEL,
                null,
                BackfillSkipPolicies.NONE,
                actionKey,
                requestedBy,
                reason);
    }

    /**
     * Create a complete-Flow backfill through the unified batch lifecycle.
     *
     * Every graph root is an explicit entry node for an admitted business date.
     * Non-root nodes remain blocked by same-instance parent snapshot confirmation.
     *
     * @param workflowCode workflow definition code
     * @param workflowVersion workflow definition version; defaults to 1 when null
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @param progressionMode PARALLEL, SERIAL, or PARALLEL_WITH_LIMIT
     * @param maxActiveDates required finite date limit for PARALLEL_WITH_LIMIT
     * @param skipPolicy NONE or SKIP_FULLY_CONFIRMED_DATES
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult backfillWorkflow(
            String workflowCode,
            Integer workflowVersion,
            LocalDate startBizDate,
            LocalDate endBizDate,
            String progressionMode,
            Integer maxActiveDates,
            String skipPolicy,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        int normalizedVersion = workflowVersion != null ? workflowVersion : 1;
        SchedulingAction action = saveAcceptedAction(SchedulingAction.builder()
                .actionKey(requireActionKey(actionKey))
                .actionType(SchedulingActionTypes.BACKFILL_WORKFLOW)
                .scopeType(SCOPE_WORKFLOW_DEFINITION)
                .workflowCode(workflowCode)
                .workflowVersion(normalizedVersion)
                .bizDateStart(startBizDate)
                .bizDateEnd(endBizDate)
                .requestedBy(requestedBy)
                .reason(reason)
                .requestPayloadJson(payload(
                        "workflowCode", workflowCode,
                        "workflowVersion", normalizedVersion,
                        "startBizDate", String.valueOf(startBizDate),
                        "endBizDate", String.valueOf(endBizDate),
                        "progressionMode", progressionMode,
                        "maxActiveDates", maxActiveDates,
                        "skipPolicy", skipPolicy))
                .build());

        BackfillBatch batch = null;
        try {
            validateBackfillRange(workflowCode, startBizDate, endBizDate);
            BackfillProgressionPolicy progressionPolicy = normalizeBackfillProgressionPolicy(
                    progressionMode,
                    maxActiveDates);
            String normalizedSkipPolicy = BackfillSkipPolicies.normalize(skipPolicy);
            FlowPlanVersion version = flowPlanVersionRepository
                    .findByFlowCodeAndVersion(workflowCode, normalizedVersion)
                    .orElseThrow(() -> new RuntimeException(
                            "FlowPlanVersion not found: " + workflowCode + " v" + normalizedVersion));
            if (!FlowPlanVersionStatuses.isPublished(version.getStatus())) {
                throw new IllegalStateException(
                        "Only published FlowPlanVersion can be backfilled: " + workflowCode + " v" + normalizedVersion);
            }
            List<ScheduleNode> nodes = flowPlanGraphService.validateAndOrder(scheduleNodeRepository
                    .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(version.getId()));
            action.setFlowPlanVersionId(version.getId());
            Set<String> entryNodeCodes = rootNodeCodes(nodes);
            batch = createBackfillBatch(
                    action,
                    version,
                    BackfillScopeTypes.FULL_FLOW,
                    null,
                    nodes,
                    entryNodeCodes,
                    startBizDate,
                    endBizDate,
                    null,
                    progressionPolicy,
                    normalizedSkipPolicy,
                    null,
                    0,
                    null,
                    requestedBy,
                    reason);
            BackfillExpansion expansion = expandBackfillBatch(
                    batch,
                    version,
                    nodes,
                    progressionPolicy,
                    SchedulingActionTypes.BACKFILL_WORKFLOW,
                    reason);
            completeBackfillBatchExpansion(
                    batch,
                    expansion.workflowInstanceIds().size(),
                    expansion.taskInstanceIds().size(),
                    expansion.skipEvidence());

            return markApplied(
                    action,
                    expansion.workflowInstanceIds(),
                    expansion.taskInstanceIds().isEmpty() ? null : expansion.taskInstanceIds().get(0),
                    "Full-flow backfill intents emitted: dates=" + countBusinessDates(startBizDate, endBizDate)
                            + ", nodes=" + nodes.size()
                            + ", progression=" + progressionPolicy.mode()
                            + ", skippedDates=" + expansion.skipEvidence().size()
                            + ", taskIntents=" + expansion.taskInstanceIds().size());
        } catch (RuntimeException e) {
            markBackfillBatchFailed(batch, e.getMessage());
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Create backfill task intents from a published FlowPlan node over a date range.
     *
     * The action expands a business-date range and a node scope into scheduler-side
     * task intents. It does not execute downstream work; every delivered intent
     * still uses target snapshot progression as the confirmation source of truth.
     *
     * @param flowPlanVersionId published FlowPlanVersion id
     * @param startNodeCode node where the backfill starts
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @param cascadePolicy NO_CASCADE, DIRECT_DOWNSTREAM, or TRANSITIVE_DOWNSTREAM
     * @param progressionMode PARALLEL, SERIAL, or PARALLEL_WITH_LIMIT
     * @param maxActiveDates required finite date limit for PARALLEL_WITH_LIMIT
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult backfillScheduleNode(
            Long flowPlanVersionId,
            String startNodeCode,
            LocalDate startBizDate,
            LocalDate endBizDate,
            String cascadePolicy,
            String progressionMode,
            Integer maxActiveDates,
            String actionKey,
            String requestedBy,
            String reason) {

        return backfillScheduleNode(
                flowPlanVersionId,
                startNodeCode,
                startBizDate,
                endBizDate,
                cascadePolicy,
                progressionMode,
                maxActiveDates,
                BackfillSkipPolicies.NONE,
                actionKey,
                requestedBy,
                reason);
    }

    /**
     * Create node backfill intents with an explicit whole-date snapshot skip policy.
     *
     * `SKIP_FULLY_CONFIRMED_DATES` skips a date only when every selected node
     * has durable `SNAPSHOT_CONFIRMED` task evidence for the same immutable
     * FlowPlan version, business date, ScheduleNode, and resolved target asset.
     * Partial node skips are forbidden because they would mix old evidence into
     * a new workflow instance and weaken DAG dependency semantics.
     *
     * @param flowPlanVersionId published FlowPlanVersion id
     * @param startNodeCode node where the backfill starts
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @param cascadePolicy node cascade policy
     * @param progressionMode business-date admission mode
     * @param maxActiveDates finite date limit for PARALLEL_WITH_LIMIT
     * @param skipPolicy NONE or SKIP_FULLY_CONFIRMED_DATES
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult backfillScheduleNode(
            Long flowPlanVersionId,
            String startNodeCode,
            LocalDate startBizDate,
            LocalDate endBizDate,
            String cascadePolicy,
            String progressionMode,
            Integer maxActiveDates,
            String skipPolicy,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = saveAcceptedAction(SchedulingAction.builder()
                .actionKey(requireActionKey(actionKey))
                .actionType(SchedulingActionTypes.BACKFILL_NODE)
                .scopeType(SCOPE_SCHEDULE_NODE)
                .flowPlanVersionId(flowPlanVersionId)
                .bizDateStart(startBizDate)
                .bizDateEnd(endBizDate)
                .requestedBy(requestedBy)
                .reason(reason)
                .requestPayloadJson(payload(
                        "flowPlanVersionId", flowPlanVersionId,
                        "startNodeCode", startNodeCode,
                        "startBizDate", String.valueOf(startBizDate),
                        "endBizDate", String.valueOf(endBizDate),
                        "cascadePolicy", cascadePolicy,
                        "progressionMode", progressionMode,
                        "maxActiveDates", maxActiveDates,
                        "skipPolicy", skipPolicy))
                .build());

        BackfillBatch batch = null;
        try {
            Long versionId = requirePositiveId(flowPlanVersionId, "flowPlanVersionId");
            String normalizedStartNodeCode = requireText(startNodeCode, "startNodeCode");
            validateBackfillRange("flowPlanVersion:" + versionId, startBizDate, endBizDate);
            String normalizedCascadePolicy = BackfillCascadePolicies.normalize(cascadePolicy);
            BackfillProgressionPolicy progressionPolicy = normalizeBackfillProgressionPolicy(
                    progressionMode,
                    maxActiveDates);
            String normalizedSkipPolicy = BackfillSkipPolicies.normalize(skipPolicy);
            FlowPlanVersion version = flowPlanVersionRepository.findById(versionId)
                    .orElseThrow(() -> new RuntimeException("FlowPlanVersion not found: " + versionId));
            if (!FlowPlanVersionStatuses.isPublished(version.getStatus())) {
                throw new IllegalStateException("Only published FlowPlanVersion can be backfilled: " + versionId);
            }

            List<ScheduleNode> versionNodes = flowPlanGraphService.validateAndOrder(scheduleNodeRepository
                    .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(versionId));
            ScheduleNode startNode = flowPlanGraphService.findNode(versionNodes, normalizedStartNodeCode);
            List<ScheduleNode> targetNodes = flowPlanGraphService.selectSubgraph(
                    versionNodes,
                    normalizedStartNodeCode,
                    normalizedCascadePolicy);
            action.setWorkflowCode(version.getFlowCode());
            action.setWorkflowVersion(version.getVersion());
            action.setScheduleNodeId(startNode.getId());
            action.setTargetAssetKey(schedulingTemplateResolver.resolve(startNode.getOutputAssetKey(), startBizDate));
            batch = createBackfillBatch(
                    action,
                    version,
                    BackfillScopeTypes.NODE_SUBGRAPH,
                    startNode,
                    targetNodes,
                    Set.of(startNode.getNodeCode()),
                    startBizDate,
                    endBizDate,
                    normalizedCascadePolicy,
                    progressionPolicy,
                    normalizedSkipPolicy,
                    null,
                    0,
                    null,
                    requestedBy,
                    reason);
            BackfillExpansion expansion = expandBackfillBatch(
                    batch,
                    version,
                    targetNodes,
                    progressionPolicy,
                    SchedulingActionTypes.BACKFILL_NODE,
                    reason);
            completeBackfillBatchExpansion(
                    batch,
                    expansion.workflowInstanceIds().size(),
                    expansion.taskInstanceIds().size(),
                    expansion.skipEvidence());

            return markApplied(
                    action,
                    expansion.workflowInstanceIds(),
                    expansion.taskInstanceIds().isEmpty() ? null : expansion.taskInstanceIds().get(0),
                    "Backfill node intents emitted: dates=" + countBusinessDates(startBizDate, endBizDate)
                            + ", nodes=" + targetNodes.size()
                            + ", progression=" + progressionPolicy.mode()
                            + ", skippedDates=" + expansion.skipEvidence().size()
                            + ", taskIntents=" + expansion.taskInstanceIds().size());
        } catch (RuntimeException e) {
            markBackfillBatchFailed(batch, e.getMessage());
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Recover a failed backfill using the compatible full-scope strategy.
     *
     * @param backfillBatchId failed source batch id
     * @param actionKey caller-provided idempotency key for the replacement request
     * @param requestedBy user or system identity requesting recovery
     * @param reason human-readable recovery reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult recoverBackfillBatch(
            Long backfillBatchId,
            String actionKey,
            String requestedBy,
            String reason) {

        return recoverBackfillBatch(
                backfillBatchId,
                BackfillRecoveryStrategies.FULL_SCOPE,
                actionKey,
                requestedBy,
                reason);
    }

    /**
     * Recover a failed backfill by creating a replacement batch from its earliest
     * snapshot-not-advanced business date through the original end date.
     *
     * FULL_SCOPE replays the source batch scope. FAILED_NODE_CASCADE starts from
     * one failed node and selects its dependency-closed descendants. The failed
     * source batch and all of its snapshot evidence remain immutable.
     *
     * @param backfillBatchId failed source batch id
     * @param recoveryStrategy full-scope or failed-node-cascade strategy
     * @param actionKey caller-provided idempotency key for the replacement request
     * @param requestedBy user or system identity requesting recovery
     * @param reason human-readable recovery reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult recoverBackfillBatch(
            Long backfillBatchId,
            String recoveryStrategy,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startBackfillControlAction(
                SchedulingActionTypes.RECOVER_BACKFILL,
                backfillBatchId,
                actionKey,
                requestedBy,
                reason);
        BackfillBatch recoveryBatch = null;
        try {
            String normalizedRecoveryStrategy = BackfillRecoveryStrategies.normalize(recoveryStrategy);
            BackfillBatch sourceBatch = lockBackfillBatch(backfillBatchId);
            copyBackfillScope(action, sourceBatch);
            requireBackfillBatchStatus(sourceBatch, BackfillBatchStatuses.FAILED);
            backfillBatchRepository.findBySourceBackfillBatchId(sourceBatch.getId()).ifPresent(existing -> {
                throw new IllegalStateException(
                        "Backfill batch already has replacement batch: " + existing.getId());
            });

            List<BackfillItem> sourceItems = backfillItemRepository
                    .findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(sourceBatch.getId());
            LocalDate recoveryStart = sourceItems.stream()
                    .filter(item -> BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED.equals(item.getStatus()))
                    .map(BackfillItem::getBizDate)
                    .min(LocalDate::compareTo)
                    .orElseThrow(() -> new IllegalStateException(
                            "Failed backfill has no SNAPSHOT_NOT_ADVANCED item: " + sourceBatch.getId()));
            if (recoveryStart.isBefore(sourceBatch.getBizDateStart())
                    || recoveryStart.isAfter(sourceBatch.getBizDateEnd())) {
                throw new IllegalStateException(
                        "Failed item business date is outside the source backfill range: " + recoveryStart);
            }

            FlowPlanVersion version = flowPlanVersionRepository.findById(sourceBatch.getFlowPlanVersionId())
                    .orElseThrow(() -> new IllegalStateException(
                            "FlowPlanVersion not found: " + sourceBatch.getFlowPlanVersionId()));
            requireStableRecoveryVersion(version);
            List<ScheduleNode> versionNodes = flowPlanGraphService.validateAndOrder(scheduleNodeRepository
                    .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(version.getId()));
            String sourceScopeType = BackfillScopeTypes.normalize(sourceBatch.getScopeType());
            List<ScheduleNode> sourceTargetNodes = resolvePersistedBackfillNodes(
                    sourceBatch,
                    versionNodes,
                    sourceScopeType);
            BackfillRecoveryScope recoveryScope = resolveBackfillRecoveryScope(
                    normalizedRecoveryStrategy,
                    sourceBatch,
                    sourceItems,
                    versionNodes,
                    sourceTargetNodes,
                    sourceScopeType);
            BackfillProgressionPolicy progressionPolicy = normalizeBackfillProgressionPolicy(
                    sourceBatch.getProgressionMode(),
                    sourceBatch.getMaxActiveDates());
            String normalizedSkipPolicy = BackfillSkipPolicies.normalize(sourceBatch.getSkipPolicy());

            action.setBizDateStart(recoveryStart);
            action.setScheduleNodeId(recoveryScope.startNode() == null
                    ? null
                    : recoveryScope.startNode().getId());
            if (recoveryScope.startNode() != null) {
                action.setTargetAssetKey(
                        schedulingTemplateResolver.resolve(
                                recoveryScope.startNode().getOutputAssetKey(),
                                recoveryStart));
            }
            action.setRequestPayloadJson(payload(
                    "sourceBackfillBatchId", sourceBatch.getId(),
                    "recoveryStrategy", normalizedRecoveryStrategy,
                    "failedNodeCodes", recoveryScope.failedNodeCodes(),
                    "scopeType", recoveryScope.scopeType(),
                    "entryNodeCodes", List.copyOf(recoveryScope.entryNodeCodes()),
                    "selectedNodeCodes", recoveryScope.targetNodes().stream()
                            .map(ScheduleNode::getNodeCode)
                            .toList(),
                    "recoveryStartBizDate", String.valueOf(recoveryStart),
                    "endBizDate", String.valueOf(sourceBatch.getBizDateEnd()),
                    "cascadePolicy", recoveryScope.cascadePolicy(),
                    "progressionMode", progressionPolicy.mode(),
                    "maxActiveDates", progressionPolicy.maxActiveDates(),
                    "skipPolicy", normalizedSkipPolicy));

            int recoveryAttempt = Optional.ofNullable(sourceBatch.getRecoveryAttempt()).orElse(0) + 1;
            recoveryBatch = createBackfillBatch(
                    action,
                    version,
                    recoveryScope.scopeType(),
                    recoveryScope.startNode(),
                    recoveryScope.targetNodes(),
                    recoveryScope.entryNodeCodes(),
                    recoveryStart,
                    sourceBatch.getBizDateEnd(),
                    recoveryScope.cascadePolicy(),
                    progressionPolicy,
                    normalizedSkipPolicy,
                    sourceBatch.getId(),
                    recoveryAttempt,
                    normalizedRecoveryStrategy,
                    requestedBy,
                    actionReason("Recover backfill batch " + sourceBatch.getId(), reason));
            BackfillExpansion expansion = expandBackfillBatch(
                    recoveryBatch,
                    version,
                    recoveryScope.targetNodes(),
                    progressionPolicy,
                    SchedulingActionTypes.RECOVER_BACKFILL,
                    reason);
            completeBackfillBatchExpansion(
                    recoveryBatch,
                    expansion.workflowInstanceIds().size(),
                    expansion.taskInstanceIds().size(),
                    expansion.skipEvidence());

            return markApplied(
                    action,
                    expansion.workflowInstanceIds(),
                    expansion.taskInstanceIds().isEmpty() ? null : expansion.taskInstanceIds().get(0),
                    "Backfill recovery intents emitted: sourceBatch=" + sourceBatch.getId()
                            + ", replacementBatch=" + recoveryBatch.getId()
                            + ", strategy=" + normalizedRecoveryStrategy
                            + ", dates=" + countBusinessDates(recoveryStart, sourceBatch.getBizDateEnd())
                            + ", nodes=" + recoveryScope.targetNodes().size()
                            + ", skippedDates=" + expansion.skipEvidence().size()
                            + ", taskIntents=" + expansion.taskInstanceIds().size());
        } catch (RuntimeException e) {
            markBackfillBatchFailed(recoveryBatch, e.getMessage());
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Pause delivery of pending scheduling intents in a backfill batch.
     *
     * Already delivered intents are unaffected and continue to be confirmed from
     * target snapshot progression.
     *
     * @param backfillBatchId batch to pause
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult pauseBackfillBatch(
            Long backfillBatchId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startBackfillControlAction(
                SchedulingActionTypes.PAUSE_BACKFILL,
                backfillBatchId,
                actionKey,
                requestedBy,
                reason);
        try {
            BackfillBatch batch = lockBackfillBatch(backfillBatchId);
            copyBackfillScope(action, batch);
            requireBackfillBatchStatus(batch, BackfillBatchStatuses.EXPANDED);
            batch.setStatus(BackfillBatchStatuses.PAUSED);
            backfillBatchRepository.save(batch);
            return markApplied(action, List.of(), null,
                    "Backfill batch paused; pending intents are hidden from delivery: " + batch.getId());
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Resume delivery of pending scheduling intents in a paused backfill batch.
     *
     * @param backfillBatchId batch to resume
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult resumeBackfillBatch(
            Long backfillBatchId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startBackfillControlAction(
                SchedulingActionTypes.RESUME_BACKFILL,
                backfillBatchId,
                actionKey,
                requestedBy,
                reason);
        try {
            BackfillBatch batch = lockBackfillBatch(backfillBatchId);
            copyBackfillScope(action, batch);
            requireBackfillBatchStatus(batch, BackfillBatchStatuses.PAUSED);
            batch.setStatus(BackfillBatchStatuses.EXPANDED);
            backfillBatchRepository.save(batch);
            backfillProgressionService.refreshBatch(batch.getId());
            return markApplied(action, List.of(), null,
                    "Backfill batch resumed; pending intents are available for delivery: " + batch.getId());
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Cancel scheduling intents that have not yet been delivered for a backfill batch.
     *
     * Delivery is scheduler-side bookkeeping. Intents already delivered to a
     * downstream system are retained and continue through snapshot confirmation;
     * this action does not claim to revoke external work.
     *
     * @param backfillBatchId batch to cancel
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult cancelBackfillBatch(
            Long backfillBatchId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startBackfillControlAction(
                SchedulingActionTypes.CANCEL_BACKFILL,
                backfillBatchId,
                actionKey,
                requestedBy,
                reason);
        try {
            BackfillBatch batch = lockBackfillBatch(backfillBatchId);
            copyBackfillScope(action, batch);
            requireBackfillBatchStatus(
                    batch,
                    BackfillBatchStatuses.EXPANDED,
                    BackfillBatchStatuses.PAUSED);

            List<BackfillItem> items = backfillItemRepository
                    .findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(batch.getId());
            List<BackfillItemTask> itemTasks = new ArrayList<>();
            for (BackfillItem item : items) {
                TaskInstance task = taskInstanceRepository.findById(item.getTaskInstanceId())
                        .orElseThrow(() -> new IllegalStateException(
                                "Backfill task instance not found: " + item.getTaskInstanceId()));
                requireKnownBackfillTaskState(task);
                itemTasks.add(new BackfillItemTask(item, task));
            }

            int cancelledPendingCount = 0;
            int retainedDeliveredCount = 0;
            for (BackfillItemTask itemTask : itemTasks) {
                BackfillItem item = itemTask.item();
                TaskInstance task = itemTask.task();
                if (isPendingDeliveryState(task.getState())) {
                    taskInstanceService.cancel(task.getId(), actionReason("Backfill batch cancelled", reason));
                    item.setStatus(BackfillItemStatuses.CANCELLED);
                    cancelledPendingCount++;
                } else if (isDeliveredState(task.getState())) {
                    item.setStatus(BackfillItemStatuses.INTENT_DELIVERED);
                    retainedDeliveredCount++;
                } else {
                    item.setStatus(BackfillItemStatuses.CANCELLED);
                }
                backfillItemRepository.save(item);
            }

            batch.setStatus(BackfillBatchStatuses.CANCELLED);
            backfillBatchRepository.save(batch);
            return markApplied(
                    action,
                    List.of(),
                    null,
                    "Backfill batch cancelled: pendingIntents=" + cancelledPendingCount
                            + ", deliveredIntentsRetained=" + retainedDeliveredCount);
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Cancel a workflow scheduling instance.
     *
     * @param workflowInstanceId workflow instance to cancel
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult cancelWorkflowInstance(
            Long workflowInstanceId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startSimpleAction(
                SchedulingActionTypes.CANCEL_WORKFLOW,
                SCOPE_WORKFLOW_INSTANCE,
                actionKey,
                workflowInstanceId,
                null,
                requestedBy,
                reason);

        try {
            WorkflowInstance workflow = workflowInstanceRepository.findByIdForUpdate(workflowInstanceId)
                    .orElseThrow(() -> new RuntimeException("Workflow instance not found: " + workflowInstanceId));
            copyWorkflowScope(action, workflow);
            List<TaskInstance> tasks = taskInstanceRepository
                    .findByWorkflowInstanceIdOrderByCreatedAtAsc(workflowInstanceId);
            int cancelledPendingIntents = 0;
            for (TaskInstance task : tasks) {
                if (isPendingDeliveryState(task.getState())) {
                    taskInstanceService.cancel(task.getId(), actionReason("Workflow instance cancelled", reason));
                    cancelledPendingIntents++;
                }
            }
            workflowInstanceService.cancel(workflowInstanceId);
            return markApplied(
                    action,
                    List.of(workflowInstanceId),
                    null,
                    "Workflow instance cancelled: pendingIntents=" + cancelledPendingIntents);
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Cancel a task scheduling instance.
     *
     * @param taskInstanceId task instance to cancel
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult cancelTaskInstance(
            Long taskInstanceId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startSimpleAction(
                SchedulingActionTypes.CANCEL_TASK,
                SCOPE_TASK_INSTANCE,
                actionKey,
                null,
                taskInstanceId,
                requestedBy,
                reason);

        try {
            TaskInstance task = taskInstanceRepository.findById(taskInstanceId)
                    .orElseThrow(() -> new RuntimeException("Task instance not found: " + taskInstanceId));
            copyTaskScope(action, task);
            taskInstanceService.cancel(taskInstanceId, reason);
            return markApplied(action, List.of(), taskInstanceId, "Task instance cancelled");
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Skip a task scheduling instance.
     *
     * @param taskInstanceId task instance to skip
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult skipTaskInstance(
            Long taskInstanceId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startSimpleAction(
                SchedulingActionTypes.SKIP_TASK,
                SCOPE_TASK_INSTANCE,
                actionKey,
                null,
                taskInstanceId,
                requestedBy,
                reason);

        try {
            TaskInstance task = taskInstanceRepository.findById(taskInstanceId)
                    .orElseThrow(() -> new RuntimeException("Task instance not found: " + taskInstanceId));
            copyTaskScope(action, task);
            taskInstanceService.markSkipped(taskInstanceId, reason);
            return markApplied(action, List.of(), taskInstanceId, "Task instance skipped");
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Recheck target snapshot progress for a scheduled task.
     *
     * @param taskInstanceId task instance to recheck
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return applied, deduplicated, or rejected action result
     */
    public SchedulingActionResult recheckTaskSnapshot(
            Long taskInstanceId,
            String actionKey,
            String requestedBy,
            String reason) {

        Optional<SchedulingActionResult> duplicate = dedupe(actionKey);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        SchedulingAction action = startSimpleAction(
                SchedulingActionTypes.RECHECK_SNAPSHOT,
                SCOPE_TASK_INSTANCE,
                actionKey,
                null,
                taskInstanceId,
                requestedBy,
                reason);

        try {
            TaskInstance task = taskInstanceRepository.findById(taskInstanceId)
                    .orElseThrow(() -> new RuntimeException("Task instance not found: " + taskInstanceId));
            copyTaskScope(action, task);
            SnapshotConfirmationResult result = snapshotConfirmationService.checkTaskSnapshotProgress(taskInstanceId, null);
            return markApplied(
                    action,
                    List.of(),
                    taskInstanceId,
                    "Snapshot recheck result: " + result.resultingState());
        } catch (RuntimeException e) {
            return markRejected(action, e.getMessage());
        }
    }

    /**
     * Return an existing result when an action key was already recorded.
     *
     * @param actionKey caller-provided idempotency key
     * @return deduplicated result when the key exists
     */
    private Optional<SchedulingActionResult> dedupe(String actionKey) {
        requireActionKey(actionKey);
        return schedulingActionRepository.findByActionKey(actionKey)
                .map(SchedulingActionResult::deduped);
    }

    /**
     * Start a simple action that targets one workflow or task instance.
     *
     * @param actionType scheduling action type
     * @param scopeType target scope type
     * @param actionKey caller-provided idempotency key
     * @param workflowInstanceId workflow instance target when present
     * @param taskInstanceId task instance target when present
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return accepted action
     */
    private SchedulingAction startSimpleAction(
            String actionType,
            String scopeType,
            String actionKey,
            Long workflowInstanceId,
            Long taskInstanceId,
            String requestedBy,
            String reason) {

        return saveAcceptedAction(SchedulingAction.builder()
                .actionKey(requireActionKey(actionKey))
                .actionType(actionType)
                .scopeType(scopeType)
                .workflowInstanceId(workflowInstanceId)
                .taskInstanceId(taskInstanceId)
                .requestedBy(requestedBy)
                .reason(reason)
                .requestPayloadJson(payload(
                        "workflowInstanceId", workflowInstanceId,
                        "taskInstanceId", taskInstanceId))
                .build());
    }

    /**
     * Persist an accepted action targeting a backfill batch.
     *
     * @param actionType backfill control action type
     * @param backfillBatchId target batch id
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable action reason
     * @return accepted action record
     */
    private SchedulingAction startBackfillControlAction(
            String actionType,
            Long backfillBatchId,
            String actionKey,
            String requestedBy,
            String reason) {

        return saveAcceptedAction(SchedulingAction.builder()
                .actionKey(requireActionKey(actionKey))
                .actionType(actionType)
                .scopeType(SCOPE_BACKFILL_BATCH)
                .requestedBy(requestedBy)
                .reason(reason)
                .requestPayloadJson(payload("backfillBatchId", backfillBatchId))
                .build());
    }

    /**
     * Lock a backfill batch while applying delivery-control changes.
     *
     * @param backfillBatchId target batch id
     * @return locked batch
     */
    private BackfillBatch lockBackfillBatch(Long backfillBatchId) {
        Long batchId = requirePositiveId(backfillBatchId, "backfillBatchId");
        return backfillBatchRepository.findByIdForUpdate(batchId)
                .orElseThrow(() -> new IllegalArgumentException("Backfill batch not found: " + batchId));
    }

    /**
     * Copy definition anchors from a batch into its control action audit record.
     *
     * @param action accepted control action
     * @param batch target backfill batch
     */
    private void copyBackfillScope(SchedulingAction action, BackfillBatch batch) {
        action.setFlowPlanVersionId(batch.getFlowPlanVersionId());
        action.setScheduleNodeId(batch.getStartScheduleNodeId());
        action.setWorkflowCode(batch.getWorkflowCode());
        action.setWorkflowVersion(batch.getWorkflowVersion());
        action.setBizDateStart(batch.getBizDateStart());
        action.setBizDateEnd(batch.getBizDateEnd());
    }

    /**
     * Copy immutable definition anchors from a workflow scheduling instance.
     *
     * @param action action receiving query anchors
     * @param workflow source workflow scheduling instance
     */
    private void copyWorkflowScope(SchedulingAction action, WorkflowInstance workflow) {
        action.setWorkflowInstanceId(workflow.getId());
        action.setWorkflowCode(workflow.getWorkflowCode());
        action.setWorkflowVersion(workflow.getWorkflowVersion());
        action.setFlowPlanVersionId(workflow.getFlowPlanVersionId());
        if (workflow.getBizDate() != null) {
            LocalDate bizDate = workflow.getBizDate().toLocalDate();
            action.setBizDateStart(bizDate);
            action.setBizDateEnd(bizDate);
        }
    }

    /**
     * Copy task and workflow definition anchors for task-scoped actions.
     *
     * The workflow lookup is best-effort for compatibility with legacy task
     * records; task-level version and node anchors are still preserved when the
     * wrapper is absent.
     *
     * @param action action receiving query anchors
     * @param task source task scheduling intent
     */
    private void copyTaskScope(SchedulingAction action, TaskInstance task) {
        WorkflowInstance workflow = workflowInstanceRepository.findById(task.getWorkflowInstanceId())
                .orElse(null);
        copyTaskScope(action, task, workflow);
    }

    /**
     * Copy task anchors using an already loaded workflow wrapper.
     *
     * @param action action receiving query anchors
     * @param task source task scheduling intent
     * @param workflow source workflow wrapper, or null for legacy orphan evidence
     */
    private void copyTaskScope(
            SchedulingAction action,
            TaskInstance task,
            WorkflowInstance workflow) {

        if (workflow != null) {
            copyWorkflowScope(action, workflow);
        } else {
            action.setWorkflowInstanceId(task.getWorkflowInstanceId());
        }
        action.setTaskInstanceId(task.getId());
        action.setFlowPlanVersionId(task.getFlowPlanVersionId());
        action.setScheduleNodeId(task.getScheduleNodeId());
        action.setTargetAssetKey(task.getTargetAssetKey());
        if (task.getBizDate() != null) {
            LocalDate bizDate = task.getBizDate().toLocalDate();
            action.setBizDateStart(bizDate);
            action.setBizDateEnd(bizDate);
        }
    }

    /**
     * Require a batch to be in one of the allowed scheduler-side control states.
     *
     * @param batch target batch
     * @param allowedStatuses allowed current statuses
     */
    private void requireBackfillBatchStatus(BackfillBatch batch, String... allowedStatuses) {
        for (String allowedStatus : allowedStatuses) {
            if (allowedStatus.equals(batch.getStatus())) {
                return;
            }
        }
        throw new IllegalStateException(
                "Backfill batch state does not allow this action: " + batch.getStatus());
    }

    /**
     * Validate that a generated task uses a scheduling state understood by batch cancellation.
     *
     * @param task generated backfill task intent
     */
    private void requireKnownBackfillTaskState(TaskInstance task) {
        if (!isPendingDeliveryState(task.getState())
                && !isDeliveredState(task.getState())
                && !SchedulingStates.CANCELLED.equals(task.getState())
                && !SchedulingStates.SKIPPED.equals(task.getState())) {
            throw new IllegalStateException(
                    "Unsupported backfill task scheduling state: " + task.getState());
        }
    }

    /**
     * Check whether an intent is still under Lakehouse Flow delivery control.
     *
     * @param state task scheduling state
     * @return true before the scheduling intent has been delivered
     */
    private boolean isPendingDeliveryState(String state) {
        return SchedulingStates.CREATED.equals(state)
                || SchedulingStates.WAITING_SNAPSHOT.equals(state)
                || SchedulingStates.READY_TO_SCHEDULE.equals(state);
    }

    /**
     * Check whether an intent was already delivered and must not be claimed as revoked.
     *
     * @param state task scheduling state
     * @return true after delivery or snapshot confirmation
     */
    private boolean isDeliveredState(String state) {
        return SchedulingStates.SCHEDULED.equals(state) || SchedulingStates.isSnapshotOutcome(state);
    }

    /**
     * Persist an accepted action before applying scheduler-side changes.
     *
     * @param action action to persist
     * @return persisted action
     */
    private SchedulingAction saveAcceptedAction(SchedulingAction action) {
        action.setStatus(SchedulingActionStatuses.ACCEPTED);
        return schedulingActionRepository.save(action);
    }

    /**
     * Mark an action as applied and return its service result.
     *
     * @param action persisted action request
     * @param workflowInstanceIds workflow instances created or affected
     * @param taskInstanceId task instance affected by the action
     * @param message human-readable result message
     * @return applied action result
     */
    private SchedulingActionResult markApplied(
            SchedulingAction action,
            List<Long> workflowInstanceIds,
            Long taskInstanceId,
            String message) {

        action.setStatus(SchedulingActionStatuses.APPLIED);
        action.setResultMessage(message);
        action.setProducedWorkflowInstanceId(workflowInstanceIds.isEmpty() ? null : workflowInstanceIds.get(0));
        action.setProducedTaskInstanceId(taskInstanceId);
        schedulingActionRepository.save(action);

        log.info("Applied scheduling action {} of type {}", action.getActionKey(), action.getActionType());
        return SchedulingActionResult.applied(action, message, workflowInstanceIds, taskInstanceId);
    }

    /**
     * Mark an action as rejected and return its service result.
     *
     * @param action persisted action request
     * @param message rejection reason
     * @return rejected action result
     */
    private SchedulingActionResult markRejected(SchedulingAction action, String message) {
        action.setStatus(SchedulingActionStatuses.REJECTED);
        action.setResultMessage(message);
        schedulingActionRepository.save(action);

        log.warn("Rejected scheduling action {} of type {}: {}",
                action.getActionKey(), action.getActionType(), message);
        return SchedulingActionResult.rejected(action, message);
    }

    /**
     * Move a generated workflow instance through scheduler decision states.
     *
     * @param workflowInstance workflow instance created or reused by an action
     */
    private void markWorkflowDecisionEmitted(WorkflowInstance workflowInstance) {
        if (SchedulingStates.CREATED.equals(workflowInstance.getState())) {
            workflowInstanceService.markSchedulable(workflowInstance.getId());
        }
    }

    /**
     * Recreate task scheduling intents for a workflow rerun.
     *
     * Published FlowPlan instances are rebuilt from their immutable graph. Legacy
     * instances without a definition anchor are cloned from their existing task
     * intents so a workflow rerun can never consist of an empty wrapper only.
     *
     * @param source source workflow scheduling instance
     * @param rerun newly created workflow scheduling instance
     * @return task scheduling intents emitted under the rerun instance
     */
    private List<TaskInstance> emitWorkflowRerunTasks(
            WorkflowInstance source,
            WorkflowInstance rerun) {

        if (source.getFlowPlanVersionId() != null) {
            FlowPlanVersion version = flowPlanVersionRepository.findById(source.getFlowPlanVersionId())
                    .orElseThrow(() -> new IllegalStateException(
                            "FlowPlanVersion not found: " + source.getFlowPlanVersionId()));
            List<ScheduleNode> nodes = flowPlanGraphService.validateAndOrder(scheduleNodeRepository
                    .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(version.getId()));
            return emitPlanTasks(
                    rerun,
                    version,
                    nodes,
                    rootNodeCodes(nodes),
                    Set.of(),
                    source.getBizDate().toLocalDate(),
                    null);
        }

        List<TaskInstance> sourceTasks = taskInstanceRepository
                .findByWorkflowInstanceIdOrderByCreatedAtAsc(source.getId());
        if (sourceTasks.isEmpty()) {
            throw new IllegalStateException(
                    "Workflow instance has no task scheduling intents to rerun: " + source.getId());
        }

        List<TaskInstance> emittedTasks = new ArrayList<>();
        for (TaskInstance sourceTask : sourceTasks) {
            TaskInstance task = taskInstanceService.createInstance(
                    rerun.getId(),
                    sourceTask.getTaskCode(),
                    sourceTask.getTaskVersion(),
                    source.getBizDate(),
                    sourceTask.getTargetAssetKey(),
                    sourceTask.getFlowPlanVersionId(),
                    sourceTask.getScheduleNodeId());
            taskInstanceService.markSchedulable(task.getId());
            emittedTasks.add(task);
        }
        return emittedTasks;
    }

    /**
     * Expand a persisted backfill batch into one workflow wrapper per business
     * date and one task scheduling intent per selected node.
     *
     * Only the selected start node may bypass its omitted upstream dependencies.
     * A date waiting for a progression slot keeps that start node blocked, while
     * every other node waits for same-instance parent snapshot confirmations.
     *
     * @param batch persisted batch that owns all generated items
     * @param version immutable FlowPlan definition version
     * @param targetNodes selected subgraph in topological order
     * @param progressionPolicy validated business-date admission policy
     * @param actionType action type used to label generated workflow intents
     * @param reason human-readable operator reason
     * @return generated workflow/task identifiers and durable whole-date skip evidence
     */
    private BackfillExpansion expandBackfillBatch(
            BackfillBatch batch,
            FlowPlanVersion version,
            List<ScheduleNode> targetNodes,
            BackfillProgressionPolicy progressionPolicy,
            String actionType,
            String reason) {

        List<Long> workflowInstanceIds = new ArrayList<>();
        List<Long> taskInstanceIds = new ArrayList<>();
        Map<String, Object> skipEvidence = new LinkedHashMap<>();
        String scopeType = BackfillScopeTypes.normalize(batch.getScopeType());
        Set<String> entryNodeCodes = resolveBackfillEntryNodeCodes(batch, targetNodes, scopeType);
        int expandedDateOffset = 0;
        for (LocalDate bizDate = batch.getBizDateStart();
                !bizDate.isAfter(batch.getBizDateEnd());
                bizDate = bizDate.plusDays(1)) {
            Optional<Map<String, Object>> dateSkipEvidence = findWholeDateSkipEvidence(
                    batch,
                    version,
                    targetNodes,
                    bizDate);
            if (dateSkipEvidence.isPresent()) {
                skipEvidence.put(String.valueOf(bizDate), dateSkipEvidence.get());
                continue;
            }
            boolean dateAdmitted = progressionPolicy.maxActiveDates() == null
                    || expandedDateOffset < progressionPolicy.maxActiveDates();
            WorkflowInstance workflow = workflowInstanceService.createInstance(
                    version.getFlowCode(),
                    version.getVersion(),
                    bizDate.atStartOfDay(),
                    SchedulingActionTypes.toTriggerType(actionType),
                    batch.getActionKey() + ":" + bizDate,
                    actionReason(backfillIntentReason(actionType, batch, bizDate), reason),
                    version.getId());
            emitPlanTasks(
                    workflow,
                    version,
                    targetNodes,
                    dateAdmitted ? entryNodeCodes : Set.of(),
                    dateAdmitted ? Set.of() : entryNodeCodes,
                    bizDate,
                    batch).forEach(task -> taskInstanceIds.add(task.getId()));
            markWorkflowDecisionEmitted(workflow);
            workflowInstanceIds.add(workflow.getId());
            expandedDateOffset++;
        }
        return new BackfillExpansion(
                List.copyOf(workflowInstanceIds),
                List.copyOf(taskInstanceIds),
                Map.copyOf(skipEvidence));
    }

    /**
     * Find complete historical snapshot-confirmation evidence for one candidate date.
     *
     * A date is skippable only when every selected node has a matching confirmed
     * task for the same version, date, node id, and resolved target asset. Missing
     * evidence for one node causes the entire date to be expanded normally.
     *
     * @param batch batch carrying the explicit skip policy
     * @param version immutable FlowPlan version
     * @param targetNodes complete selected subgraph
     * @param bizDate candidate business date
     * @return node-keyed evidence when the complete date may be skipped
     */
    private Optional<Map<String, Object>> findWholeDateSkipEvidence(
            BackfillBatch batch,
            FlowPlanVersion version,
            List<ScheduleNode> targetNodes,
            LocalDate bizDate) {

        if (!BackfillSkipPolicies.SKIP_FULLY_CONFIRMED_DATES.equals(batch.getSkipPolicy())) {
            return Optional.empty();
        }
        List<TaskInstance> candidates = taskInstanceRepository
                .findByFlowPlanVersionIdAndBizDateAndStateOrderByUpdatedAtDesc(
                        version.getId(),
                        bizDate.atStartOfDay(),
                        SchedulingStates.SNAPSHOT_CONFIRMED);
        Map<String, Object> evidenceByNode = new LinkedHashMap<>();
        for (ScheduleNode node : targetNodes) {
            String targetAssetKey = schedulingTemplateResolver.resolve(node.getOutputAssetKey(), bizDate);
            Optional<TaskInstance> evidence = candidates.stream()
                    .filter(task -> Objects.equals(node.getId(), task.getScheduleNodeId()))
                    .filter(task -> Objects.equals(targetAssetKey, task.getTargetAssetKey()))
                    .filter(task -> !isBlank(task.getObservedSnapshotId()))
                    .findFirst();
            if (evidence.isEmpty()) {
                return Optional.empty();
            }
            TaskInstance task = evidence.get();
            evidenceByNode.put(node.getNodeCode(), payload(
                    "taskInstanceId", task.getId(),
                    "targetAssetKey", task.getTargetAssetKey(),
                    "baselineSnapshotId", task.getBaselineSnapshotId(),
                    "observedSnapshotId", task.getObservedSnapshotId()));
        }
        return Optional.of(Map.copyOf(evidenceByNode));
    }

    /**
     * Build the audit reason for an original or recovery backfill workflow.
     *
     * @param actionType action that owns the generated intent
     * @param batch owning unified backfill batch
     * @param bizDate business date represented by the workflow
     * @return stable scheduler-side reason text
     */
    private String backfillIntentReason(String actionType, BackfillBatch batch, LocalDate bizDate) {
        String operation = SchedulingActionTypes.RECOVER_BACKFILL.equals(actionType)
                ? "Recover backfill "
                : "Backfill ";
        String scope = BackfillScopeTypes.FULL_FLOW.equals(batch.getScopeType())
                ? "full flow " + batch.getWorkflowCode()
                : "nodes from " + batch.getStartNodeCode();
        return operation + scope + " for " + bizDate;
    }

    /**
     * Emit one date-scoped set of task intents from a topologically ordered graph.
     *
     * Entry nodes are explicit scheduling starts and become immediately
     * deliverable. Every other node waits until {@link DagProgressionService}
     * observes snapshot confirmation for all direct upstream nodes.
     *
     * @param workflow owning workflow scheduling instance
     * @param version immutable FlowPlan version
     * @param nodes selected graph nodes in topological order
     * @param entryNodeCodes nodes allowed to start without upstream confirmation
     * @param concurrencyQueuedNodeCodes entry nodes waiting for a batch date slot
     * @param bizDate business date used to resolve asset templates
     * @param backfillBatch optional backfill batch used to persist item progress
     * @return generated task scheduling intents
     */
    private List<TaskInstance> emitPlanTasks(
            WorkflowInstance workflow,
            FlowPlanVersion version,
            List<ScheduleNode> nodes,
            Set<String> entryNodeCodes,
            Set<String> concurrencyQueuedNodeCodes,
            LocalDate bizDate,
            BackfillBatch backfillBatch) {

        List<TaskInstance> emittedTasks = new ArrayList<>();
        for (ScheduleNode node : nodes) {
            String targetAssetKey = schedulingTemplateResolver.resolve(node.getOutputAssetKey(), bizDate);
            TaskInstance task = taskInstanceService.createInstance(
                    workflow.getId(),
                    node.getNodeCode(),
                    version.getVersion(),
                    bizDate.atStartOfDay(),
                    targetAssetKey,
                    version.getId(),
                    node.getId());
            boolean entryNode = entryNodeCodes.contains(node.getNodeCode());
            boolean concurrencyQueued = concurrencyQueuedNodeCodes.contains(node.getNodeCode());
            if (entryNode) {
                taskInstanceService.markSchedulable(task.getId());
            } else {
                taskInstanceService.markWaitingForSnapshot(
                        task.getId(),
                        concurrencyQueued ? waitingForBackfillSlot(backfillBatch) : waitingForDagDependencies(node));
            }
            if (backfillBatch != null) {
                saveBackfillItem(
                        backfillBatch,
                        bizDate,
                        version.getId(),
                        node,
                        workflow,
                        task,
                        targetAssetKey,
                        entryNode
                                ? BackfillItemStatuses.INTENT_READY
                                : concurrencyQueued
                                        ? BackfillItemStatuses.WAITING_CONCURRENCY
                                        : BackfillItemStatuses.WAITING_DEPENDENCY);
            }
            emittedTasks.add(task);
        }
        return emittedTasks;
    }

    /**
     * Resolve graph roots that may start a full-workflow scheduling action.
     *
     * @param nodes validated FlowPlan nodes
     * @return root node codes
     */
    private Set<String> rootNodeCodes(List<ScheduleNode> nodes) {
        return nodes.stream()
                .filter(node -> node.getDependsOnNodes() == null || node.getDependsOnNodes().isEmpty())
                .map(ScheduleNode::getNodeCode)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Select the immutable graph scope for a replacement backfill batch.
     *
     * @param recoveryStrategy normalized replacement strategy
     * @param sourceBatch failed source batch
     * @param sourceItems source batch item evidence
     * @param versionNodes complete immutable FlowPlan graph
     * @param sourceTargetNodes source batch selected node range
     * @param sourceScopeType normalized source scope
     * @return replacement batch graph scope
     */
    private BackfillRecoveryScope resolveBackfillRecoveryScope(
            String recoveryStrategy,
            BackfillBatch sourceBatch,
            List<BackfillItem> sourceItems,
            List<ScheduleNode> versionNodes,
            List<ScheduleNode> sourceTargetNodes,
            String sourceScopeType) {

        List<String> failedNodeCodes = sourceItems.stream()
                .filter(item -> BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED.equals(item.getStatus()))
                .map(BackfillItem::getNodeCode)
                .filter(code -> code != null && !code.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
        if (BackfillRecoveryStrategies.FULL_SCOPE.equals(recoveryStrategy)) {
            Set<String> sourceEntries = resolveBackfillEntryNodeCodes(
                    sourceBatch,
                    sourceTargetNodes,
                    sourceScopeType);
            ScheduleNode sourceStartNode = BackfillScopeTypes.NODE_SUBGRAPH.equals(sourceScopeType)
                    ? flowPlanGraphService.findNode(versionNodes, sourceBatch.getStartNodeCode())
                    : null;
            return new BackfillRecoveryScope(
                    sourceScopeType,
                    sourceStartNode,
                    sourceTargetNodes,
                    sourceEntries,
                    sourceBatch.getCascadePolicy(),
                    failedNodeCodes);
        }

        if (failedNodeCodes.size() != 1) {
            throw new IllegalStateException(
                    "FAILED_NODE_CASCADE recovery requires exactly one failed node; use FULL_SCOPE for "
                            + failedNodeCodes.size() + " failed nodes");
        }
        String failedNodeCode = failedNodeCodes.get(0);
        ScheduleNode failedNode = flowPlanGraphService.findNode(versionNodes, failedNodeCode);
        List<ScheduleNode> recoveryNodes = selectRecoveryDescendants(sourceTargetNodes, failedNodeCode);
        Set<String> recoveryEntries = Set.of(failedNodeCode);
        requireBackfillDependencyClosure(recoveryNodes, recoveryEntries);
        return new BackfillRecoveryScope(
                BackfillScopeTypes.NODE_SUBGRAPH,
                failedNode,
                recoveryNodes,
                recoveryEntries,
                BackfillCascadePolicies.TRANSITIVE_DOWNSTREAM,
                failedNodeCodes);
    }

    /**
     * Select one failed node and every reachable descendant inside the source scope.
     *
     * @param sourceTargetNodes source batch nodes in topological order
     * @param failedNodeCode explicit failed recovery entry
     * @return failed-node recovery scope in topological order
     */
    private List<ScheduleNode> selectRecoveryDescendants(
            List<ScheduleNode> sourceTargetNodes,
            String failedNodeCode) {

        boolean failedNodeInSource = sourceTargetNodes.stream()
                .anyMatch(node -> failedNodeCode.equals(node.getNodeCode()));
        if (!failedNodeInSource) {
            throw new IllegalStateException(
                    "Failed node is outside the source backfill scope: " + failedNodeCode);
        }

        Set<String> selectedCodes = new LinkedHashSet<>();
        selectedCodes.add(failedNodeCode);
        for (ScheduleNode node : sourceTargetNodes) {
            List<String> dependencies = node.getDependsOnNodes() == null
                    ? List.of()
                    : node.getDependsOnNodes();
            if (dependencies.stream().anyMatch(selectedCodes::contains)) {
                selectedCodes.add(node.getNodeCode());
            }
        }
        return sourceTargetNodes.stream()
                .filter(node -> selectedCodes.contains(node.getNodeCode()))
                .toList();
    }

    /**
     * Resolve the immutable node range saved on a source batch for recovery.
     *
     * Batches written before scope unification fall back to their original
     * complete-flow or node-subgraph definition fields.
     *
     * @param batch source batch
     * @param versionNodes complete immutable FlowPlan graph in topological order
     * @param scopeType normalized batch scope
     * @return selected nodes in graph order
     */
    private List<ScheduleNode> resolvePersistedBackfillNodes(
            BackfillBatch batch,
            List<ScheduleNode> versionNodes,
            String scopeType) {

        List<String> persistedCodes = batch.getSelectedNodeCodes();
        if (persistedCodes == null || persistedCodes.isEmpty()) {
            if (BackfillScopeTypes.FULL_FLOW.equals(scopeType)) {
                return versionNodes;
            }
            return flowPlanGraphService.selectSubgraph(
                    versionNodes,
                    batch.getStartNodeCode(),
                    batch.getCascadePolicy());
        }

        Set<String> selectedCodes = persistedCodes.stream()
                .map(code -> requireText(code, "selectedNodeCode"))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<ScheduleNode> selectedNodes = versionNodes.stream()
                .filter(node -> selectedCodes.contains(node.getNodeCode()))
                .toList();
        if (selectedNodes.size() != selectedCodes.size()) {
            Set<String> foundCodes = selectedNodes.stream()
                    .map(ScheduleNode::getNodeCode)
                    .collect(Collectors.toSet());
            List<String> missingCodes = selectedCodes.stream()
                    .filter(code -> !foundCodes.contains(code))
                    .toList();
            throw new IllegalStateException(
                    "Backfill batch references nodes missing from its FlowPlanVersion: "
                            + String.join(",", missingCodes));
        }
        return selectedNodes;
    }

    /**
     * Resolve and validate all explicit date-entry nodes for one unified batch.
     *
     * @param batch owning or source batch
     * @param selectedNodes immutable selected node range
     * @param scopeType normalized batch scope
     * @return entry node codes in selected graph order
     */
    private Set<String> resolveBackfillEntryNodeCodes(
            BackfillBatch batch,
            List<ScheduleNode> selectedNodes,
            String scopeType) {

        List<String> persistedEntries = batch.getEntryNodeCodes();
        Set<String> entries;
        if (persistedEntries == null || persistedEntries.isEmpty()) {
            entries = BackfillScopeTypes.FULL_FLOW.equals(scopeType)
                    ? rootNodeCodes(selectedNodes)
                    : Set.of(requireText(batch.getStartNodeCode(), "startNodeCode"));
        } else {
            entries = persistedEntries.stream()
                    .map(code -> requireText(code, "entryNodeCode"))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        List<String> orderedEntries = orderedNodeCodes(selectedNodes, entries);
        requireBackfillDependencyClosure(selectedNodes, Set.copyOf(orderedEntries));
        return new LinkedHashSet<>(orderedEntries);
    }

    /**
     * Return selected node codes in graph order while validating a requested subset.
     *
     * @param selectedNodes selected graph nodes in topological order
     * @param requestedCodes node codes that must belong to the selected graph
     * @return requested codes in selected graph order
     */
    private List<String> orderedNodeCodes(List<ScheduleNode> selectedNodes, Set<String> requestedCodes) {
        List<String> orderedCodes = selectedNodes.stream()
                .map(ScheduleNode::getNodeCode)
                .filter(requestedCodes::contains)
                .toList();
        if (orderedCodes.size() != requestedCodes.size()) {
            throw new IllegalStateException("Backfill entry nodes must belong to the selected node scope");
        }
        if (orderedCodes.isEmpty()) {
            throw new IllegalStateException("Backfill batch must contain at least one entry node");
        }
        return orderedCodes;
    }

    /**
     * Require every non-entry node to retain all direct parents in the selected scope.
     *
     * @param selectedNodes selected graph nodes
     * @param entryNodeCodes nodes explicitly allowed to bypass omitted parents
     */
    private void requireBackfillDependencyClosure(
            List<ScheduleNode> selectedNodes,
            Set<String> entryNodeCodes) {

        Set<String> selectedCodes = selectedNodes.stream()
                .map(ScheduleNode::getNodeCode)
                .collect(Collectors.toSet());
        for (ScheduleNode node : selectedNodes) {
            if (entryNodeCodes.contains(node.getNodeCode())) {
                continue;
            }
            List<String> dependencies = node.getDependsOnNodes() == null
                    ? List.of()
                    : node.getDependsOnNodes();
            List<String> missingDependencies = dependencies.stream()
                    .filter(code -> !selectedCodes.contains(code))
                    .toList();
            if (!missingDependencies.isEmpty()) {
                throw new IllegalStateException(
                        "Backfill batch node " + node.getNodeCode()
                                + " is missing direct parents " + String.join(",", missingDependencies));
            }
        }
    }

    /**
     * Build a stable operator-facing reason for a DAG-blocked node.
     *
     * @param node node waiting for its direct upstream snapshot confirmations
     * @return waiting reason stored on the task intent
     */
    private String waitingForDagDependencies(ScheduleNode node) {
        List<String> dependencies = node.getDependsOnNodes() == null ? List.of() : node.getDependsOnNodes();
        return "Waiting for upstream snapshot confirmation: " + String.join(",", dependencies);
    }

    /**
     * Build a stable reason for an entry node waiting for a date-admission slot.
     *
     * @param batch owning backfill batch
     * @return waiting reason stored on the task intent
     */
    private String waitingForBackfillSlot(BackfillBatch batch) {
        return "Waiting for backfill date slot: batch=" + batch.getId()
                + ", mode=" + batch.getProgressionMode()
                + ", maxActiveDates=" + batch.getMaxActiveDates();
    }

    /**
     * Persist a backfill batch before expanding node/date items.
     *
     * @param action accepted scheduling action
     * @param version published FlowPlanVersion
     * @param scopeType complete-flow or node-subgraph scope
     * @param startNode node where a NODE_SUBGRAPH backfill starts, or null for FULL_FLOW
     * @param selectedNodes immutable nodes expanded for every date
     * @param entryNodeCodes selected nodes allowed to bypass graph parents
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @param cascadePolicy normalized cascade policy
     * @param progressionPolicy validated date-admission policy
     * @param skipPolicy normalized whole-date skip policy
     * @param sourceBackfillBatchId failed batch replaced by this batch, or null for an original request
     * @param recoveryAttempt recovery depth in the replacement chain
     * @param recoveryStrategy replacement strategy, or null for an original batch
     * @param requestedBy requester identity
     * @param reason human-readable reason
     * @return persisted backfill batch
     */
    private BackfillBatch createBackfillBatch(
            SchedulingAction action,
            FlowPlanVersion version,
            String scopeType,
            ScheduleNode startNode,
            List<ScheduleNode> selectedNodes,
            Set<String> entryNodeCodes,
            LocalDate startBizDate,
            LocalDate endBizDate,
            String cascadePolicy,
            BackfillProgressionPolicy progressionPolicy,
            String skipPolicy,
            Long sourceBackfillBatchId,
            Integer recoveryAttempt,
            String recoveryStrategy,
            String requestedBy,
            String reason) {

        String normalizedScopeType = validateBackfillScope(
                scopeType,
                startNode,
                cascadePolicy,
                selectedNodes,
                entryNodeCodes);
        return backfillBatchRepository.save(BackfillBatch.builder()
                .batchKey(action.getActionKey())
                .actionKey(action.getActionKey())
                .flowPlanVersionId(version.getId())
                .workflowCode(version.getFlowCode())
                .workflowVersion(version.getVersion())
                .scopeType(normalizedScopeType)
                .entryNodeCodes(orderedNodeCodes(selectedNodes, entryNodeCodes))
                .selectedNodeCodes(selectedNodes.stream().map(ScheduleNode::getNodeCode).toList())
                .startScheduleNodeId(startNode == null ? null : startNode.getId())
                .startNodeCode(startNode == null ? null : startNode.getNodeCode())
                .bizDateStart(startBizDate)
                .bizDateEnd(endBizDate)
                .cascadePolicy(cascadePolicy)
                .progressionMode(progressionPolicy.mode())
                .maxActiveDates(progressionPolicy.maxActiveDates())
                .skipPolicy(skipPolicy)
                .skippedDateCount(0)
                .skipEvidenceJson(Map.of())
                .sourceBackfillBatchId(sourceBackfillBatchId)
                .recoveryAttempt(recoveryAttempt)
                .recoveryStrategy(recoveryStrategy)
                .status(BackfillBatchStatuses.CREATED)
                .producedWorkflowCount(0)
                .totalItemCount(0)
                .requestedBy(requestedBy)
                .reason(reason)
                .build());
    }

    /**
     * Validate scope-specific fields before a unified backfill batch is persisted.
     *
     * @param scopeType requested complete-flow or node-subgraph scope
     * @param startNode optional node-subgraph start node
     * @param cascadePolicy optional node-subgraph cascade policy
     * @param selectedNodes immutable selected graph nodes
     * @param entryNodeCodes explicit date-entry nodes
     * @return normalized scope type
     */
    private String validateBackfillScope(
            String scopeType,
            ScheduleNode startNode,
            String cascadePolicy,
            List<ScheduleNode> selectedNodes,
            Set<String> entryNodeCodes) {

        String normalizedScopeType = BackfillScopeTypes.normalize(scopeType);
        if (BackfillScopeTypes.FULL_FLOW.equals(normalizedScopeType)) {
            if (startNode != null || cascadePolicy != null) {
                throw new IllegalArgumentException("FULL_FLOW backfill cannot declare a start node or cascade policy");
            }
        } else if (startNode == null || isBlank(cascadePolicy)) {
            throw new IllegalArgumentException("NODE_SUBGRAPH backfill requires a start node and cascade policy");
        }
        List<String> orderedEntries = orderedNodeCodes(selectedNodes, entryNodeCodes);
        requireBackfillDependencyClosure(selectedNodes, Set.copyOf(orderedEntries));
        return normalizedScopeType;
    }

    /**
     * Persist one generated node/date backfill item.
     *
     * @param batch owning backfill batch
     * @param bizDate business date for this item
     * @param flowPlanVersionId published FlowPlanVersion id
     * @param node generated ScheduleNode
     * @param workflow workflow wrapper generated for the date
     * @param task task intent generated for the node/date
     * @param targetAssetKey date-resolved target asset key
     * @param status initial backfill item status
     */
    private void saveBackfillItem(
            BackfillBatch batch,
            LocalDate bizDate,
            Long flowPlanVersionId,
            ScheduleNode node,
            WorkflowInstance workflow,
            TaskInstance task,
            String targetAssetKey,
            String status) {

        backfillItemRepository.save(BackfillItem.builder()
                .backfillBatchId(batch.getId())
                .bizDate(bizDate)
                .flowPlanVersionId(flowPlanVersionId)
                .scheduleNodeId(node.getId())
                .nodeCode(node.getNodeCode())
                .targetAssetKey(targetAssetKey)
                .workflowInstanceId(workflow.getId())
                .taskInstanceId(task.getId())
                .status(status)
                .build());
    }

    /**
     * Complete expansion bookkeeping for generated and safely skipped dates.
     *
     * @param batch batch to update
     * @param producedWorkflowCount number of workflow wrappers generated
     * @param totalItemCount number of task intents generated
     * @param skipEvidence date-keyed historical snapshot-confirmation evidence
     */
    private void completeBackfillBatchExpansion(
            BackfillBatch batch,
            int producedWorkflowCount,
            int totalItemCount,
            Map<String, Object> skipEvidence) {

        batch.setStatus(producedWorkflowCount == 0
                ? BackfillBatchStatuses.COMPLETED
                : BackfillBatchStatuses.EXPANDED);
        batch.setProducedWorkflowCount(producedWorkflowCount);
        batch.setTotalItemCount(totalItemCount);
        batch.setSkippedDateCount(skipEvidence.size());
        batch.setSkipEvidenceJson(skipEvidence);
        backfillBatchRepository.save(batch);
    }

    /**
     * Mark a partially created batch as failed when expansion is rejected.
     *
     * @param batch batch to update; ignored when null
     * @param message failure message
     */
    private void markBackfillBatchFailed(BackfillBatch batch, String message) {
        if (batch != null) {
            batch.setStatus(BackfillBatchStatuses.FAILED);
            batch.setReason(actionReason("Backfill expansion failed", message));
            backfillBatchRepository.save(batch);
        }
    }

    /**
     * Validate the workflow and date range for a backfill action.
     *
     * @param workflowCode workflow code supplied by the caller
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     */
    private void validateBackfillRange(String workflowCode, LocalDate startBizDate, LocalDate endBizDate) {
        if (isBlank(workflowCode)) {
            throw new IllegalArgumentException("workflowCode is required");
        }
        if (startBizDate == null || endBizDate == null) {
            throw new IllegalArgumentException("Backfill date range is required");
        }
        if (endBizDate.isBefore(startBizDate)) {
            throw new IllegalArgumentException("Backfill endBizDate must be on or after startBizDate");
        }
    }

    /**
     * Validate and normalize one node-backfill date progression policy.
     *
     * Blank mode preserves the historical unrestricted parallel behavior.
     * SERIAL always has one active date, while PARALLEL_WITH_LIMIT requires an
     * explicit positive limit.
     *
     * @param progressionMode requested progression mode
     * @param maxActiveDates requested finite active-date limit
     * @return normalized policy persisted with the backfill batch
     */
    private BackfillProgressionPolicy normalizeBackfillProgressionPolicy(
            String progressionMode,
            Integer maxActiveDates) {

        String mode = isBlank(progressionMode)
                ? BackfillProgressionModes.PARALLEL
                : progressionMode.trim().toUpperCase(Locale.ROOT);
        return switch (mode) {
            case BackfillProgressionModes.PARALLEL -> {
                if (maxActiveDates != null) {
                    throw new IllegalArgumentException("maxActiveDates is only valid for PARALLEL_WITH_LIMIT");
                }
                yield new BackfillProgressionPolicy(mode, null);
            }
            case BackfillProgressionModes.SERIAL -> {
                if (maxActiveDates != null && maxActiveDates != 1) {
                    throw new IllegalArgumentException("SERIAL backfill requires maxActiveDates to be 1 or omitted");
                }
                yield new BackfillProgressionPolicy(mode, 1);
            }
            case BackfillProgressionModes.PARALLEL_WITH_LIMIT -> {
                if (maxActiveDates == null || maxActiveDates <= 0) {
                    throw new IllegalArgumentException(
                            "PARALLEL_WITH_LIMIT backfill requires positive maxActiveDates");
                }
                yield new BackfillProgressionPolicy(mode, maxActiveDates);
            }
            default -> throw new IllegalArgumentException("Unsupported backfill progressionMode: " + mode);
        };
    }

    /**
     * Require a recovery definition to be immutable and previously published.
     *
     * Retired versions remain valid recovery anchors because their graph is
     * immutable and the recovery must reproduce the exact original semantics.
     * Draft versions are never accepted as historical scheduling evidence.
     *
     * @param version FlowPlanVersion referenced by the failed batch
     */
    private void requireStableRecoveryVersion(FlowPlanVersion version) {
        if (!FlowPlanVersionStatuses.isPublished(version.getStatus())
                && !FlowPlanVersionStatuses.RETIRED.equals(version.getStatus())) {
            throw new IllegalStateException(
                    "Backfill recovery requires a published or retired FlowPlanVersion: " + version.getId());
        }
    }

    /**
     * Count business dates in an inclusive date range.
     *
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @return number of dates in the range
     */
    private long countBusinessDates(LocalDate startBizDate, LocalDate endBizDate) {
        return startBizDate.datesUntil(endBizDate.plusDays(1)).count();
    }

    /**
     * Require a positive identifier for action targets.
     *
     * @param id candidate id
     * @param fieldName field name for validation messages
     * @return validated id
     */
    private Long requirePositiveId(Long id, String fieldName) {
        if (id == null || id <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return id;
    }

    /**
     * Require a business date for date-scoped actions.
     *
     * @param bizDate candidate business date
     * @return validated business date
     */
    private LocalDate requireBizDate(LocalDate bizDate) {
        if (bizDate == null) {
            throw new IllegalArgumentException("bizDate is required");
        }
        return bizDate;
    }

    /**
     * Require a non-empty idempotency key for action processing.
     *
     * @param actionKey caller-provided action key
     * @return validated action key
     */
    private String requireActionKey(String actionKey) {
        if (isBlank(actionKey)) {
            throw new IllegalArgumentException("actionKey is required for scheduling actions");
        }
        return actionKey;
    }

    /**
     * Require a non-empty text field.
     *
     * @param value candidate text
     * @param fieldName field name for validation messages
     * @return trimmed text
     */
    private String requireText(String value, String fieldName) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        return value.trim();
    }

    /**
     * Prefix a detail message with an optional operator reason.
     *
     * @param detail action detail
     * @param reason optional caller reason
     * @return reason string stored on generated workflow instances
     */
    private String actionReason(String detail, String reason) {
        if (isBlank(reason)) {
            return detail;
        }
        return detail + ": " + reason;
    }

    /**
     * Build a JSON-compatible payload from alternating key/value pairs.
     *
     * @param keyValues alternating string keys and arbitrary values
     * @return payload map with null values omitted
     */
    private Map<String, Object> payload(Object... keyValues) {
        Map<String, Object> payload = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            Object key = keyValues[i];
            Object value = keyValues[i + 1];
            if (key != null && value != null) {
                payload.put(String.valueOf(key), value);
            }
        }
        return payload;
    }

    /**
     * Check whether a string is null or only whitespace.
     *
     * @param value source value
     * @return true when value has no meaningful text
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
