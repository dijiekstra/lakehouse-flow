package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.FlowPlanVersionStatuses;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.TriggerHistory;
import io.github.lakehouseflow.model.WorkflowInstance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Evaluates published FlowPlan roots and emits snapshot-driven DAG intents.
 *
 * The service creates scheduling records only. Root conditions are joined from
 * current AssetState evidence, downstream DAG nodes wait for upstream target
 * snapshots, and no executor or runtime status is consulted.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class FlowPlanEvaluationService {

    private static final String TRIGGER_TYPE = "SNAPSHOT_DRIVEN";

    private final FlowPlanVersionRepository flowPlanVersionRepository;
    private final ScheduleNodeRepository scheduleNodeRepository;
    private final FlowPlanGraphService flowPlanGraphService;
    private final FlowPlanConditionService flowPlanConditionService;
    private final SchedulingTemplateResolver schedulingTemplateResolver;
    private final WorkflowInstanceService workflowInstanceService;
    private final TaskInstanceService taskInstanceService;
    private final TriggerHistoryService triggerHistoryService;
    private final FlowPlanDecisionMetrics flowPlanDecisionMetrics;

    /**
     * Evaluate every published snapshot-driven FlowPlan after an asset advances.
     *
     * A changed asset can trigger a plan only when it is referenced by a root
     * node input. All roots must then satisfy their external conditions before
     * one complete DAG scheduling instance is emitted.
     *
     * @param assetKey changed asset key
     * @param snapshotId changed asset snapshot id
     * @param bizDate event business time
     * @return outcomes for plans whose root dependencies referenced the asset
     */
    public List<FlowPlanTriggerOutcome> evaluateTriggeredAsset(
            String assetKey,
            String snapshotId,
            LocalDateTime bizDate) {

        return evaluateTriggeredAssets(
                assetKey == null ? List.of() : List.of(assetKey),
                snapshotId,
                bizDate);
    }

    /**
     * Evaluate one snapshot against every table and partition state it changed.
     *
     * Each published FlowPlan version is evaluated at most once even when its
     * root conditions reference more than one changed asset scope.
     *
     * @param assetKeys changed table and partition asset keys
     * @param snapshotId changed snapshot id
     * @param bizDate event business time
     * @return outcomes for plans whose root dependencies reference any changed asset
     */
    public List<FlowPlanTriggerOutcome> evaluateTriggeredAssets(
            Collection<String> assetKeys,
            String snapshotId,
            LocalDateTime bizDate) {

        LocalDateTime effectiveBizTime = bizDate != null ? bizDate : LocalDateTime.now();
        LocalDate effectiveBizDate = effectiveBizTime.toLocalDate();
        List<String> normalizedAssetKeys = assetKeys == null
                ? List.of()
                : assetKeys.stream()
                        .filter(assetKey -> assetKey != null && !assetKey.isBlank())
                        .map(String::trim)
                        .distinct()
                        .sorted()
                        .toList();
        if (normalizedAssetKeys.isEmpty()) {
            return List.of();
        }
        List<FlowPlanTriggerOutcome> outcomes = new ArrayList<>();
        for (FlowPlanVersion version : flowPlanVersionRepository
                .findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED)) {
            if (!allowsSnapshotTrigger(version)) {
                flowPlanDecisionMetrics.recordInspection(FlowPlanInspectionOutcome.TRIGGER_POLICY_FILTERED);
                continue;
            }
            List<ScheduleNode> nodes = flowPlanGraphService.validateAndOrder(scheduleNodeRepository
                    .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(version.getId()));
            List<ScheduleNode> roots = nodes.stream().filter(this::isRoot).toList();
            Optional<String> matchedAssetKey = normalizedAssetKeys.stream()
                    .filter(assetKey -> roots.stream()
                            .map(node -> effectiveDependencySpec(version, node))
                            .anyMatch(spec -> flowPlanConditionService.referencesAsset(
                                    spec,
                                    effectiveBizDate,
                                    assetKey)))
                    .findFirst();
            if (matchedAssetKey.isEmpty()) {
                flowPlanDecisionMetrics.recordInspection(FlowPlanInspectionOutcome.ASSET_UNMATCHED);
                continue;
            }
            flowPlanDecisionMetrics.recordInspection(FlowPlanInspectionOutcome.MATCHED);
            outcomes.add(evaluateAndEmit(
                    version,
                    nodes,
                    roots,
                    matchedAssetKey.get(),
                    snapshotId,
                    effectiveBizTime));
        }
        return outcomes;
    }

    /**
     * Evaluate root gates and emit one immutable-version scheduling instance.
     *
     * @param version published FlowPlan version
     * @param nodes validated nodes in topological order
     * @param roots graph root nodes
     * @param assetKey changed asset key
     * @param snapshotId changed snapshot id
     * @param bizDate effective business time
     * @return one plan evaluation outcome
     */
    private FlowPlanTriggerOutcome evaluateAndEmit(
            FlowPlanVersion version,
            List<ScheduleNode> nodes,
            List<ScheduleNode> roots,
            String assetKey,
            String snapshotId,
            LocalDateTime bizDate) {

        long startedAt = System.nanoTime();
        String triggerKey = buildTriggerKey(version, assetKey, snapshotId, bizDate);
        try {
            Optional<TriggerHistory> existing = triggerHistoryService.findByTriggerKey(triggerKey);
            if (existing.isPresent()) {
                FlowPlanTriggerOutcome outcome = FlowPlanTriggerOutcome.deduped(
                        version,
                        triggerKey,
                        existing.get().getWorkflowInstanceId());
                recordDecision(outcome, assetKey, snapshotId, startedAt);
                return outcome;
            }

            List<EvaluationResult> rootResults = roots.stream()
                    .map(node -> flowPlanConditionService.evaluate(
                            effectiveDependencySpec(version, node),
                            bizDate.toLocalDate()))
                    .toList();
            Optional<EvaluationResult> blocked = rootResults.stream()
                    .filter(result -> !Boolean.TRUE.equals(result.getSatisfied()))
                    .findFirst();
            if (blocked.isPresent()) {
                String reason = blocked.get().getWaitingReason();
                triggerHistoryService.recordSkippedTrigger(
                        triggerKey,
                        TRIGGER_TYPE,
                        assetKey,
                        snapshotId,
                        reason);
                FlowPlanTriggerOutcome outcome = FlowPlanTriggerOutcome.skipped(version, triggerKey, reason);
                recordDecision(outcome, assetKey, snapshotId, startedAt);
                return outcome;
            }

            EvaluationResult evaluation = EvaluationResult.builder()
                    .satisfied(true)
                    .assetKey(assetKey)
                    .snapshotId(snapshotId)
                    .description("All FlowPlan root snapshot dependencies satisfied")
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
            WorkflowInstance workflow = workflowInstanceService.createInstance(
                    version.getFlowCode(),
                    version.getVersion(),
                    bizDate,
                    TRIGGER_TYPE,
                    triggerKey,
                    evaluation.getDescription(),
                    version.getId());
            Set<String> rootCodes = roots.stream().map(ScheduleNode::getNodeCode).collect(Collectors.toSet());
            List<TaskInstance> tasks = emitTasks(version, workflow, nodes, rootCodes, bizDate.toLocalDate());
            workflowInstanceService.markSchedulable(workflow.getId());
            triggerHistoryService.recordWorkflowTrigger(triggerKey, TRIGGER_TYPE, evaluation, workflow.getId());
            if (!tasks.isEmpty()) {
                triggerHistoryService.recordTaskTrigger(
                        triggerKey + ":first-task",
                        TRIGGER_TYPE,
                        evaluation,
                        tasks.get(0).getId());
            }
            FlowPlanTriggerOutcome outcome = FlowPlanTriggerOutcome.emitted(
                    version,
                    triggerKey,
                    workflow.getId(),
                    tasks.isEmpty() ? null : tasks.get(0).getId());
            recordDecision(outcome, assetKey, snapshotId, startedAt);
            return outcome;
        } catch (RuntimeException e) {
            flowPlanDecisionMetrics.recordDecision(
                    FlowPlanTriggerDecision.FAILED,
                    Duration.ofNanos(System.nanoTime() - startedAt));
            log.error(
                    "FlowPlan trigger decision=FAILED flowPlanVersionId={} flowCode={} assetKey={} "
                            + "snapshotId={} triggerKey={} reason={}",
                    version.getId(), version.getFlowCode(), assetKey, snapshotId, triggerKey, e.getMessage(), e);
            throw e;
        }
    }

    /** Record bounded metrics and high-cardinality structured evidence for one trigger decision. */
    private void recordDecision(
            FlowPlanTriggerOutcome outcome,
            String assetKey,
            String snapshotId,
            long startedAt) {
        flowPlanDecisionMetrics.recordDecision(
                outcome.decision(),
                Duration.ofNanos(System.nanoTime() - startedAt));
        log.info(
                "FlowPlan trigger decision={} flowPlanVersionId={} flowCode={} assetKey={} snapshotId={} "
                        + "triggerKey={} workflowInstanceId={} reason={}",
                outcome.decision(), outcome.flowPlanVersionId(), outcome.flowCode(), assetKey, snapshotId,
                outcome.triggerKey(), outcome.workflowInstanceId(), outcome.reason());
    }

    /**
     * Emit root-ready and downstream-waiting task intents for one DAG instance.
     *
     * @param version immutable plan version
     * @param workflow owning workflow scheduling instance
     * @param nodes topologically ordered nodes
     * @param rootCodes root node codes
     * @param bizDate business date for target template resolution
     * @return generated task intents
     */
    private List<TaskInstance> emitTasks(
            FlowPlanVersion version,
            WorkflowInstance workflow,
            List<ScheduleNode> nodes,
            Set<String> rootCodes,
            LocalDate bizDate) {

        List<TaskInstance> tasks = new ArrayList<>();
        for (ScheduleNode node : nodes) {
            TaskInstance task = taskInstanceService.createInstance(
                    workflow.getId(),
                    node.getNodeCode(),
                    version.getVersion(),
                    bizDate.atStartOfDay(),
                    schedulingTemplateResolver.resolve(node.getOutputAssetKey(), bizDate),
                    version.getId(),
                    node.getId());
            if (rootCodes.contains(node.getNodeCode())) {
                taskInstanceService.markSchedulable(task.getId());
            } else {
                taskInstanceService.markWaitingForSnapshot(
                        task.getId(),
                        "Waiting for upstream snapshot confirmation: "
                                + String.join(",", node.getDependsOnNodes()));
            }
            tasks.add(task);
        }
        return tasks;
    }

    /**
     * Resolve node-level dependencies with FlowPlan-version defaults.
     *
     * @param version plan version
     * @param node schedule node
     * @return effective input dependency specification
     */
    private Map<String, Object> effectiveDependencySpec(FlowPlanVersion version, ScheduleNode node) {
        Map<String, Object> nodeSpec = node.getInputDependencySpecJson();
        return nodeSpec == null || nodeSpec.isEmpty() ? version.getDependencySpecJson() : nodeSpec;
    }

    /**
     * Check whether a version permits snapshot-driven triggering.
     *
     * Empty policies retain the prototype's snapshot-driven default. A declared
     * `type` must explicitly be `SNAPSHOT_DRIVEN`.
     *
     * @param version plan version
     * @return true when snapshot events may trigger the plan
     */
    private boolean allowsSnapshotTrigger(FlowPlanVersion version) {
        Map<String, Object> policy = version.getTriggerPolicyJson();
        if (policy == null || policy.isEmpty() || policy.get("type") == null) {
            return true;
        }
        return TRIGGER_TYPE.equals(policy.get("type").toString().toUpperCase(Locale.ROOT));
    }

    /**
     * Check whether a node is a DAG root.
     *
     * @param node schedule node
     * @return true when no internal upstream node is declared
     */
    private boolean isRoot(ScheduleNode node) {
        return node.getDependsOnNodes() == null || node.getDependsOnNodes().isEmpty();
    }

    /**
     * Build a compact idempotency key for one version and source snapshot.
     *
     * @param version evaluated plan version
     * @param assetKey changed asset key
     * @param snapshotId changed snapshot id
     * @param bizDate event business time
     * @return durable trigger key
     */
    private String buildTriggerKey(
            FlowPlanVersion version,
            String assetKey,
            String snapshotId,
            LocalDateTime bizDate) {
        String raw = String.join(":",
                String.valueOf(version.getId()),
                String.valueOf(assetKey),
                String.valueOf(snapshotId),
                String.valueOf(bizDate));
        return "flowplan-snapshot:" + version.getId() + ":" + shortHash(raw);
    }

    /**
     * Hash trigger input into a short stable suffix.
     *
     * @param value raw trigger identity
     * @return first eight SHA-256 bytes as hexadecimal text
     */
    private String shortHash(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    /**
     * Snapshot-trigger result for one published FlowPlan version.
     *
     * @param flowPlanVersionId evaluated version id
     * @param flowCode FlowPlan code
     * @param triggerKey scheduling decision idempotency key
     * @param schedulingIntentEmitted whether a new DAG instance was emitted
     * @param workflowInstanceId emitted or existing workflow instance id
     * @param firstTaskInstanceId first emitted task id for trace convenience
     * @param reason decision reason
     * @param decision bounded result used for metrics and structured logs
     */
    public record FlowPlanTriggerOutcome(
            Long flowPlanVersionId,
            String flowCode,
            String triggerKey,
            boolean schedulingIntentEmitted,
            Long workflowInstanceId,
            Long firstTaskInstanceId,
            String reason,
            FlowPlanTriggerDecision decision) {

        /**
         * Build an emitted outcome.
         *
         * @param version evaluated version
         * @param triggerKey trigger key
         * @param workflowInstanceId generated workflow id
         * @param firstTaskInstanceId first generated task id
         * @return emitted outcome
         */
        public static FlowPlanTriggerOutcome emitted(
                FlowPlanVersion version,
                String triggerKey,
                Long workflowInstanceId,
                Long firstTaskInstanceId) {
            return new FlowPlanTriggerOutcome(
                    version.getId(),
                    version.getFlowCode(),
                    triggerKey,
                    true,
                    workflowInstanceId,
                    firstTaskInstanceId,
                    "FlowPlan DAG scheduling intents emitted",
                    FlowPlanTriggerDecision.EMITTED);
        }

        /**
         * Build a skipped outcome for unsatisfied root conditions.
         *
         * @param version evaluated version
         * @param triggerKey trigger key
         * @param reason waiting reason
         * @return skipped outcome
         */
        public static FlowPlanTriggerOutcome skipped(
                FlowPlanVersion version,
                String triggerKey,
                String reason) {
            return new FlowPlanTriggerOutcome(
                    version.getId(), version.getFlowCode(), triggerKey, false, null, null, reason,
                    FlowPlanTriggerDecision.BLOCKED_CONDITION);
        }

        /**
         * Build an idempotent outcome for an existing trigger decision.
         *
         * @param version evaluated version
         * @param triggerKey trigger key
         * @param workflowInstanceId existing workflow id
         * @return deduplicated outcome
         */
        public static FlowPlanTriggerOutcome deduped(
                FlowPlanVersion version,
                String triggerKey,
                Long workflowInstanceId) {
            return new FlowPlanTriggerOutcome(
                    version.getId(),
                    version.getFlowCode(),
                    triggerKey,
                    false,
                    workflowInstanceId,
                    null,
                    "Trigger already recorded",
                    FlowPlanTriggerDecision.DEDUPLICATED);
        }
    }
}
