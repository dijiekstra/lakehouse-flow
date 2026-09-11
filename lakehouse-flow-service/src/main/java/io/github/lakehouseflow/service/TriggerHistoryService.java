package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.TriggerHistoryRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.TriggerHistory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service for recording trigger history.
 *
 * Maintains audit log of why workflow and task instances were created.
 * Used for debugging, auditing, and tracing data flow.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class TriggerHistoryService {

    private final TriggerHistoryRepository triggerHistoryRepository;

    /**
     * Record a trigger event for workflow instance creation.
     *
     * @param triggerKey Unique trigger key for idempotency
     * @param triggerType SNAPSHOT_DRIVEN, SCHEDULED, MANUAL, BACKFILL
     * @param evaluationResult Evaluation result that triggered the creation
     * @param workflowInstanceId ID of the created workflow instance
     */
    public void recordWorkflowTrigger(
            String triggerKey,
            String triggerType,
            EvaluationResult evaluationResult,
            Long workflowInstanceId) {

        // Check if already recorded (idempotency)
        Optional<TriggerHistory> existing = triggerHistoryRepository.findByTriggerKey(triggerKey);
        if (existing.isPresent()) {
            log.debug("Trigger {} already recorded, skipping", triggerKey);
            return;
        }

        TriggerHistory history = TriggerHistory.builder()
                .triggerKey(triggerKey)
                .triggerType(triggerType)
                .assetKey(evaluationResult.getAssetKey())
                .snapshotId(evaluationResult.getSnapshotId())
                .watermark(evaluationResult.getWatermark() != null ? 
                    LocalDateTime.parse(evaluationResult.getWatermark()) : null)
                .eventId(evaluationResult.getEventId())
                .workflowInstanceId(workflowInstanceId)
                .decision(evaluationResult.getSatisfied() ? "TRIGGERED" : "SKIPPED")
                .decisionReason(evaluationResult.getWaitingReason() != null ? 
                    evaluationResult.getWaitingReason() : evaluationResult.getDescription())
                .evaluationPayloadJson(evaluationResult.toString())
                .createdAt(LocalDateTime.now())
                .build();

        triggerHistoryRepository.save(history);
        log.info("Recorded workflow trigger {} for instance {}", triggerKey, workflowInstanceId);
    }

    /**
     * Record a trigger event for task instance creation.
     *
     * @param triggerKey Unique trigger key for idempotency
     * @param triggerType SNAPSHOT_DRIVEN, SCHEDULED, MANUAL
     * @param evaluationResult Evaluation result that triggered the creation
     * @param taskInstanceId ID of the created task instance
     */
    public void recordTaskTrigger(
            String triggerKey,
            String triggerType,
            EvaluationResult evaluationResult,
            Long taskInstanceId) {

        // Check if already recorded (idempotency)
        Optional<TriggerHistory> existing = triggerHistoryRepository.findByTriggerKey(triggerKey);
        if (existing.isPresent()) {
            log.debug("Trigger {} already recorded, skipping", triggerKey);
            return;
        }

        TriggerHistory history = TriggerHistory.builder()
                .triggerKey(triggerKey)
                .triggerType(triggerType)
                .assetKey(evaluationResult.getAssetKey())
                .snapshotId(evaluationResult.getSnapshotId())
                .watermark(evaluationResult.getWatermark() != null ? 
                    LocalDateTime.parse(evaluationResult.getWatermark()) : null)
                .eventId(evaluationResult.getEventId())
                .taskInstanceId(taskInstanceId)
                .decision(evaluationResult.getSatisfied() ? "TRIGGERED" : "SKIPPED")
                .decisionReason(evaluationResult.getWaitingReason() != null ? 
                    evaluationResult.getWaitingReason() : evaluationResult.getDescription())
                .evaluationPayloadJson(evaluationResult.toString())
                .createdAt(LocalDateTime.now())
                .build();

        triggerHistoryRepository.save(history);
        log.info("Recorded task trigger {} for instance {}", triggerKey, taskInstanceId);
    }

    /**
     * Record a skipped trigger (dependency not satisfied).
     */
    public void recordSkippedTrigger(
            String triggerKey,
            String triggerType,
            String assetKey,
            String snapshotId,
            String waitingReason) {

        // Check if already recorded
        Optional<TriggerHistory> existing = triggerHistoryRepository.findByTriggerKey(triggerKey);
        if (existing.isPresent()) {
            return;
        }

        TriggerHistory history = TriggerHistory.builder()
                .triggerKey(triggerKey)
                .triggerType(triggerType)
                .assetKey(assetKey)
                .snapshotId(snapshotId)
                .decision("SKIPPED")
                .decisionReason(waitingReason)
                .createdAt(LocalDateTime.now())
                .build();

        triggerHistoryRepository.save(history);
        log.debug("Recorded skipped trigger {} because {}", triggerKey, waitingReason);
    }

    /**
     * Find why a workflow instance was created.
     */
    public List<TriggerHistory> findWorkflowTriggers(Long workflowInstanceId) {
        return triggerHistoryRepository.findByWorkflowInstanceId(workflowInstanceId);
    }

    /**
     * Find why a task instance was created.
     */
    public List<TriggerHistory> findTaskTriggers(Long taskInstanceId) {
        return triggerHistoryRepository.findByTaskInstanceId(taskInstanceId);
    }

    /**
     * Find all snapshots that triggered workflow creation.
     * Used for tracing: which data caused which workflow?
     */
    public List<TriggerHistory> findSnapshotTriggers(String snapshotId) {
        return triggerHistoryRepository.findBySnapshotIdOrderByCreatedAtDesc(snapshotId);
    }

    /**
     * Find all workflow triggers for an asset.
     * Used for understanding which workflows depend on which assets.
     */
    public List<TriggerHistory> findAssetTriggers(String assetKey) {
        return triggerHistoryRepository.findByAssetKeyOrderByCreatedAtDesc(assetKey);
    }
}
