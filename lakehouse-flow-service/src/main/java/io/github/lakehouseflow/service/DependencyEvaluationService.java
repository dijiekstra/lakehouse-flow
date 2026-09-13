package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.AssetDependencyRepository;
import io.github.lakehouseflow.model.AssetDependency;
import io.github.lakehouseflow.model.DependencyCondition;
import io.github.lakehouseflow.model.DependencyEvaluationOutcome;
import io.github.lakehouseflow.model.DependencySpec;
import io.github.lakehouseflow.model.EvaluationResult;
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
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/**
 * Evaluates AssetDependency records and emits scheduling intents.
 *
 * This service is still scheduling-only: it creates workflow/task audit records
 * and records TriggerHistory. The internal outbox publisher captures target
 * baselines later; no downstream runtime result is consumed here.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class DependencyEvaluationService {

    private static final String TRIGGER_TYPE_SNAPSHOT = "SNAPSHOT_DRIVEN";

    private final AssetDependencyRepository assetDependencyRepository;
    private final ConditionEvaluator conditionEvaluator;
    private final WorkflowInstanceService workflowInstanceService;
    private final TaskInstanceService taskInstanceService;
    private final TriggerHistoryService triggerHistoryService;

    /**
     * Evaluate dependencies that directly reference a changed asset.
     */
    public List<DependencyEvaluationOutcome> evaluateTriggeredAsset(
            String assetKey,
            String snapshotId,
            LocalDateTime bizDate) {

        return assetDependencyRepository.findByAssetKeyAndEnabledTrueOrderByCreatedAtAsc(assetKey).stream()
                .map(dependency -> evaluateAndEmit(dependency, assetKey, snapshotId, bizDate))
                .toList();
    }

    /**
     * Evaluate one dependency and create scheduling-side instances when ready.
     */
    public DependencyEvaluationOutcome evaluateAndEmit(
            AssetDependency dependency,
            String triggerAssetKey,
            String triggerSnapshotId,
            LocalDateTime bizDate) {

        String triggerKey = buildTriggerKey(dependency, triggerAssetKey, triggerSnapshotId, bizDate);
        Optional<TriggerHistory> existingTrigger = triggerHistoryService.findByTriggerKey(triggerKey);
        if (existingTrigger.isPresent()) {
            return DependencyEvaluationOutcome.deduped(dependency, existingTrigger.get());
        }

        EvaluationResult evaluation = evaluateDependency(dependency, triggerAssetKey, triggerSnapshotId);
        if (!Boolean.TRUE.equals(evaluation.getSatisfied())) {
            triggerHistoryService.recordSkippedTrigger(
                    triggerKey,
                    TRIGGER_TYPE_SNAPSHOT,
                    triggerAssetKey,
                    triggerSnapshotId,
                    evaluation.getWaitingReason());
            return DependencyEvaluationOutcome.skipped(dependency, triggerKey, evaluation.getWaitingReason());
        }

        if (isBlank(dependency.getWorkflowCode())) {
            String reason = "Dependency satisfied but workflowCode is missing";
            triggerHistoryService.recordSkippedTrigger(
                    triggerKey,
                    TRIGGER_TYPE_SNAPSHOT,
                    triggerAssetKey,
                    triggerSnapshotId,
                    reason);
            return DependencyEvaluationOutcome.skipped(dependency, triggerKey, reason);
        }

        DependencySpec spec = DependencySpec.from(dependency.getDependencyConditions(), dependency.getAssetKey());
        LocalDateTime effectiveBizDate = bizDate != null ? bizDate : LocalDateTime.now();

        WorkflowInstance workflowInstance = workflowInstanceService.createInstance(
                dependency.getWorkflowCode(),
                spec.workflowVersion(),
                effectiveBizDate,
                TRIGGER_TYPE_SNAPSHOT,
                triggerKey,
                evaluation.getDescription());
        workflowInstanceService.markSchedulable(workflowInstance.getId());

        TaskInstance taskInstance = null;
        String resultingState = SchedulingStates.READY_TO_SCHEDULE;

        if (!isBlank(dependency.getTaskCode())) {
            taskInstance = taskInstanceService.createInstance(
                    workflowInstance.getId(),
                    dependency.getTaskCode(),
                    spec.taskVersion(),
                    effectiveBizDate,
                    spec.targetAssetKey());
            taskInstanceService.markSchedulable(taskInstance.getId());
        } else {
            workflowInstanceService.markScheduled(workflowInstance.getId());
            resultingState = SchedulingStates.SCHEDULED;
        }

        triggerHistoryService.recordWorkflowTrigger(
                triggerKey,
                TRIGGER_TYPE_SNAPSHOT,
                evaluation,
                workflowInstance.getId());
        if (taskInstance != null) {
            triggerHistoryService.recordTaskTrigger(
                    triggerKey + ":task",
                    TRIGGER_TYPE_SNAPSHOT,
                    evaluation,
                    taskInstance.getId());
        }

        log.info("Dependency {} created a ready scheduling decision for workflow {} task {}",
                dependency.getId(), dependency.getWorkflowCode(), dependency.getTaskCode());

        return DependencyEvaluationOutcome.emitted(
                dependency,
                triggerKey,
                workflowInstance,
                taskInstance,
                resultingState,
                evaluation.getDescription());
    }

    /**
     * Evaluate whether a dependency spec is satisfied by current asset-state evidence.
     *
     * @param dependency dependency definition to evaluate
     * @param triggerAssetKey asset key that caused the current evaluation
     * @param triggerSnapshotId snapshot id that caused the current evaluation
     * @return aggregated dependency evaluation result
     */
    EvaluationResult evaluateDependency(
            AssetDependency dependency,
            String triggerAssetKey,
            String triggerSnapshotId) {

        DependencySpec spec = DependencySpec.from(dependency.getDependencyConditions(), dependency.getAssetKey());
        if (spec.conditions().isEmpty()) {
            return EvaluationResult.unsatisfied("Dependency conditions are empty");
        }

        List<EvaluationResult> results = spec.conditions().stream()
                .map(this::evaluateCondition)
                .toList();

        boolean satisfied = isDependencySatisfied(spec, results);

        EvaluationResult firstResult = results.isEmpty() ? EvaluationResult.unsatisfied("No dependency conditions")
                : results.get(0);
        String reason = satisfied
                ? "Dependency conditions satisfied for " + dependency.getAssetKey()
                : results.stream()
                        .filter(result -> !Boolean.TRUE.equals(result.getSatisfied()))
                        .map(EvaluationResult::getWaitingReason)
                        .filter(reasonText -> !isBlank(reasonText))
                        .findFirst()
                        .orElse("Dependency conditions are not satisfied");

        return EvaluationResult.builder()
                .satisfied(satisfied)
                .assetKey(triggerAssetKey)
                .snapshotId(triggerSnapshotId != null ? triggerSnapshotId : firstResult.getSnapshotId())
                .watermark(firstResult.getWatermark())
                .eventId(firstResult.getEventId())
                .waitingReason(satisfied ? null : reason)
                .description(reason)
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Evaluate one normalized dependency condition.
     *
     * @param condition normalized condition from DependencySpec
     * @return condition evaluation result
     */
    private EvaluationResult evaluateCondition(DependencyCondition condition) {
        return conditionEvaluator.evaluateCondition(condition.type(), condition.assetKey(), condition.value());
    }

    /**
     * Combine condition results according to the dependency operator.
     *
     * @param spec normalized dependency spec
     * @param results condition-level evaluation results
     * @return whether the dependency is satisfied
     */
    private boolean isDependencySatisfied(DependencySpec spec, List<EvaluationResult> results) {
        if (spec.usesOrOperator()) {
            return results.stream().anyMatch(result -> Boolean.TRUE.equals(result.getSatisfied()));
        }
        if (spec.usesAndOperator()) {
            return results.stream().allMatch(result -> Boolean.TRUE.equals(result.getSatisfied()));
        }
        return false;
    }

    /**
     * Build an idempotency key for a snapshot-driven scheduling decision.
     *
     * @param dependency dependency being evaluated
     * @param triggerAssetKey changed input asset key
     * @param triggerSnapshotId changed input snapshot id
     * @param bizDate business time associated with the triggering snapshot
     * @return stable trigger key
     */
    private String buildTriggerKey(
            AssetDependency dependency,
            String triggerAssetKey,
            String triggerSnapshotId,
            LocalDateTime bizDate) {

        String rawKey = String.join(":",
                "snapshot",
                String.valueOf(dependency.getId()),
                safe(dependency.getWorkflowCode()),
                safe(dependency.getTaskCode()),
                safe(triggerAssetKey),
                safe(triggerSnapshotId),
                String.valueOf(bizDate));
        return "snapshot:" + dependency.getId() + ":" + shortHash(rawKey);
    }

    /**
     * Hash a raw trigger identity into a compact stable suffix.
     *
     * @param value raw trigger identity
     * @return first 8 bytes of SHA-256 encoded as hex
     */
    private String shortHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    /**
     * Convert a nullable string into a stable trigger-key component.
     *
     * @param value source value
     * @return source value or a placeholder when absent
     */
    private String safe(String value) {
        return value != null ? value : "-";
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
