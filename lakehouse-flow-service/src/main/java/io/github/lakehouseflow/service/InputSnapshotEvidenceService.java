package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.InputSnapshotEvidence;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Evaluates mixed STREAMING/BATCH DAG inputs and builds the immutable intent input vector.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class InputSnapshotEvidenceService {

    private static final String ASSET_SNAPSHOT = "ASSET_SNAPSHOT";
    private static final String SCHEDULE_INSTANCE_OUTPUT = "SCHEDULE_INSTANCE_OUTPUT";
    private static final String EXTERNAL_ASSET = "EXTERNAL_ASSET";

    private final FlowPlanVersionRepository flowPlanVersionRepository;
    private final ScheduleNodeRepository scheduleNodeRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final AssetStateRepository assetStateRepository;
    private final FlowPlanConditionService flowPlanConditionService;
    private final SchedulingTemplateResolver schedulingTemplateResolver;

    /**
     * Evaluate every direct parent edge and external dependency for one task.
     *
     * <p>Ordinary streaming parents contribute AssetState evidence and do not need a task.
     * Batch parents, plus streaming parents created by an explicit replay action, must contribute
     * a snapshot-confirmed task from the same workflow instance.
     *
     * @param task candidate task scheduling decision
     * @return processing mode, readiness decision, and frozen input evidence
     */
    public InputEvidenceEvaluation evaluate(TaskInstance task) {
        if (task == null) {
            throw new IllegalArgumentException("task must not be null");
        }
        if (task.getFlowPlanVersionId() == null || task.getScheduleNodeId() == null) {
            return InputEvidenceEvaluation.satisfied(ScheduleNodeProcessingModes.BATCH, List.of());
        }
        if (task.getBizDate() == null) {
            return InputEvidenceEvaluation.blocked(
                    ScheduleNodeProcessingModes.BATCH,
                    "Task has no business date for input evidence");
        }

        FlowPlanVersion version = flowPlanVersionRepository.findById(task.getFlowPlanVersionId())
                .orElseThrow(() -> new IllegalStateException(
                        "FlowPlanVersion not found: " + task.getFlowPlanVersionId()));
        List<ScheduleNode> nodes = scheduleNodeRepository
                .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(version.getId());
        Map<Long, ScheduleNode> nodesById = nodes.stream()
                .collect(Collectors.toMap(ScheduleNode::getId, Function.identity()));
        ScheduleNode node = nodesById.get(task.getScheduleNodeId());
        if (node == null) {
            throw new IllegalStateException("ScheduleNode not found: " + task.getScheduleNodeId());
        }
        String processingMode = ScheduleNodeProcessingModes.normalize(node.getProcessingMode());
        if (task.isParentDependencyBypassed()) {
            return InputEvidenceEvaluation.satisfied(processingMode, List.of());
        }

        Map<String, ScheduleNode> nodesByCode = nodes.stream()
                .collect(Collectors.toMap(ScheduleNode::getNodeCode, Function.identity()));
        Map<String, TaskInstance> tasksByNodeCode = taskInstanceRepository
                .findByWorkflowInstanceIdOrderByCreatedAtAsc(task.getWorkflowInstanceId()).stream()
                .filter(candidate -> candidate.getScheduleNodeId() != null)
                .filter(candidate -> nodesById.containsKey(candidate.getScheduleNodeId()))
                .collect(Collectors.toMap(
                        candidate -> nodesById.get(candidate.getScheduleNodeId()).getNodeCode(),
                        Function.identity()));

        List<InputSnapshotEvidence> evidence = new ArrayList<>();
        for (String parentCode : dependencies(node)) {
            ScheduleNode parentNode = nodesByCode.get(parentCode);
            if (parentNode == null) {
                return InputEvidenceEvaluation.blocked(
                        processingMode,
                        "Missing direct parent node: " + parentCode);
            }
            InputEvidenceEvaluation parentResult = evaluateParent(
                    task,
                    parentNode,
                    tasksByNodeCode.get(parentCode));
            if (!parentResult.satisfied()) {
                return InputEvidenceEvaluation.blocked(processingMode, parentResult.waitingReason());
            }
            evidence.addAll(parentResult.evidence());
        }

        FlowPlanConditionService.DependencyEvaluation external = flowPlanConditionService.evaluateWithEvidence(
                effectiveDependencySpec(version, node),
                task.getBizDate().toLocalDate());
        if (!Boolean.TRUE.equals(external.aggregate().getSatisfied())) {
            return InputEvidenceEvaluation.blocked(processingMode, external.aggregate().getWaitingReason());
        }
        evidence.addAll(externalEvidence(external.satisfiedEvidence()));
        return InputEvidenceEvaluation.satisfied(processingMode, evidence);
    }

    /** Evaluate one direct parent using same-instance task evidence or a streaming asset state. */
    private InputEvidenceEvaluation evaluateParent(
            TaskInstance candidate,
            ScheduleNode parentNode,
            TaskInstance parentTask) {
        String parentMode = ScheduleNodeProcessingModes.normalize(parentNode.getProcessingMode());
        if (parentTask != null) {
            if (!SchedulingStates.SNAPSHOT_CONFIRMED.equals(parentTask.getState())) {
                return InputEvidenceEvaluation.blocked(
                        parentMode,
                        "Waiting for parent snapshot confirmation: " + parentNode.getNodeCode());
            }
            if (isBlank(parentTask.getObservedSnapshotId())) {
                return InputEvidenceEvaluation.blocked(
                        parentMode,
                        "Confirmed parent has no observed snapshot: " + parentNode.getNodeCode());
            }
            return InputEvidenceEvaluation.satisfied(parentMode, List.of(new InputSnapshotEvidence(
                    parentNode.getNodeCode(),
                    parentMode,
                    SCHEDULE_INSTANCE_OUTPUT,
                    parentTask.getTargetAssetKey(),
                    parentTask.getObservedSnapshotId(),
                    null,
                    parentTask.getId(),
                    firstNonNull(parentTask.getLastSnapshotCheckAt(), parentTask.getUpdatedAt()))));
        }
        if (!ScheduleNodeProcessingModes.STREAMING.equals(parentMode)) {
            return InputEvidenceEvaluation.blocked(
                    parentMode,
                    "Missing same-instance batch parent task: " + parentNode.getNodeCode());
        }
        String parentAssetKey = schedulingTemplateResolver.resolve(
                parentNode.getOutputAssetKey(),
                candidate.getBizDate().toLocalDate());
        if (isBlank(parentAssetKey)) {
            return InputEvidenceEvaluation.blocked(
                    parentMode,
                    "Streaming parent has no output asset: " + parentNode.getNodeCode());
        }
        AssetState state = assetStateRepository.findByAssetKey(parentAssetKey).orElse(null);
        if (!hasBusinessPosition(state)) {
            return InputEvidenceEvaluation.blocked(
                    parentMode,
                    "Waiting for streaming parent asset evidence: " + parentAssetKey);
        }
        return InputEvidenceEvaluation.satisfied(parentMode, List.of(new InputSnapshotEvidence(
                parentNode.getNodeCode(),
                parentMode,
                ASSET_SNAPSHOT,
                parentAssetKey,
                state.getLatestDataSnapshotId(),
                format(state.getLatestDataWatermark()),
                null,
                state.getUpdatedAt())));
    }

    /** Convert satisfied external conditions into one evidence entry per input asset. */
    private List<InputSnapshotEvidence> externalEvidence(List<EvaluationResult> conditionEvidence) {
        Map<String, InputSnapshotEvidence> byAsset = new LinkedHashMap<>();
        for (EvaluationResult result : conditionEvidence) {
            AssetState state = assetStateRepository.findByAssetKey(result.getAssetKey()).orElse(null);
            String snapshotId = result.getSnapshotId() != null
                    ? result.getSnapshotId()
                    : state == null ? null : state.getLatestDataSnapshotId();
            String watermark = result.getWatermark() != null
                    ? result.getWatermark()
                    : state == null ? null : format(state.getLatestDataWatermark());
            LocalDateTime observedAt = state == null ? null : state.getUpdatedAt();
            byAsset.putIfAbsent(result.getAssetKey(), new InputSnapshotEvidence(
                    null,
                    null,
                    EXTERNAL_ASSET,
                    result.getAssetKey(),
                    snapshotId,
                    watermark,
                    null,
                    observedAt));
        }
        return List.copyOf(byAsset.values());
    }

    /** Resolve node-level external dependencies with the version default as fallback. */
    private Map<String, Object> effectiveDependencySpec(FlowPlanVersion version, ScheduleNode node) {
        Map<String, Object> nodeSpec = node.getInputDependencySpecJson();
        return nodeSpec == null || nodeSpec.isEmpty() ? version.getDependencySpecJson() : nodeSpec;
    }

    /** Return normalized direct parent codes in definition order. */
    private List<String> dependencies(ScheduleNode node) {
        if (node.getDependsOnNodes() == null) {
            return List.of();
        }
        return node.getDependsOnNodes().stream()
                .filter(code -> code != null && !code.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
    }

    /** Check whether an AssetState carries a business snapshot or watermark. */
    private boolean hasBusinessPosition(AssetState state) {
        return state != null
                && (state.getLatestDataSnapshotId() != null || state.getLatestDataWatermark() != null);
    }

    /** Format one optional watermark for the JSON contract. */
    private String format(LocalDateTime value) {
        return value == null ? null : value.toString();
    }

    /** Return the first non-null audit timestamp. */
    private LocalDateTime firstNonNull(LocalDateTime first, LocalDateTime second) {
        return first != null ? first : second;
    }

    /** Check whether text is null or blank. */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Engine-neutral dependency decision and the evidence frozen when it is satisfied.
     *
     * @param processingMode candidate node processing mode
     * @param satisfied whether every required input is available
     * @param evidence complete input snapshot/watermark vector
     * @param waitingReason missing dependency explanation, or null
     */
    public record InputEvidenceEvaluation(
            String processingMode,
            boolean satisfied,
            List<InputSnapshotEvidence> evidence,
            String waitingReason) {

        /** Build a satisfied dependency decision. */
        private static InputEvidenceEvaluation satisfied(
                String processingMode,
                List<InputSnapshotEvidence> evidence) {
            return new InputEvidenceEvaluation(processingMode, true, List.copyOf(evidence), null);
        }

        /** Build a blocked dependency decision. */
        private static InputEvidenceEvaluation blocked(String processingMode, String waitingReason) {
            return new InputEvidenceEvaluation(processingMode, false, List.of(), waitingReason);
        }
    }
}
