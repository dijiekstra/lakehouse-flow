package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Read-only service for querying scheduler-side backfill audit records.
 *
 * Backfill batches and items describe how Lakehouse Flow expanded a historical
 * date range into scheduling intents. They do not describe external executor
 * runtime or task completion.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class BackfillQueryService {

    private final BackfillBatchRepository backfillBatchRepository;
    private final BackfillItemRepository backfillItemRepository;

    /**
     * Backfill batch read model.
     *
     * @param id batch id
     * @param batchKey stable batch idempotency key
     * @param actionKey scheduling action key that created the batch
     * @param flowPlanVersionId published FlowPlanVersion used for expansion
     * @param workflowCode workflow code copied from the FlowPlanVersion
     * @param workflowVersion workflow version copied from the FlowPlanVersion
     * @param scopeType complete-flow or node-subgraph scope
     * @param entryNodeCodes nodes allowed to start each admitted business date
     * @param selectedNodeCodes immutable nodes expanded for every business date
     * @param startScheduleNodeId ScheduleNode where a node-subgraph expansion started
     * @param startNodeCode node code where a node-subgraph expansion started
     * @param bizDateStart inclusive start business date
     * @param bizDateEnd inclusive end business date
     * @param cascadePolicy cascade policy used during expansion
     * @param progressionMode business-date scheduling progression mode
     * @param maxActiveDates finite active-date limit, or null for unrestricted parallel progression
     * @param skipPolicy whole-date snapshot confirmation skip policy
     * @param skippedDateCount number of dates omitted with complete confirmation evidence
     * @param skipEvidence date-keyed node/task snapshot confirmation evidence
     * @param sourceBackfillBatchId failed batch replaced by this batch, or null for an original request
     * @param recoveryAttempt recovery depth in the replacement chain
     * @param recoveryStrategy replacement strategy, or null for an original batch
     * @param status scheduler-side batch status
     * @param producedWorkflowCount generated workflow wrapper count
     * @param totalItemCount generated task intent count
     * @param requestedBy user or system that requested the backfill
     * @param reason human-readable request reason
     * @param createdAt record creation timestamp
     * @param updatedAt record update timestamp
     */
    public record BackfillBatchView(
            Long id,
            String batchKey,
            String actionKey,
            Long flowPlanVersionId,
            String workflowCode,
            Integer workflowVersion,
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
            Integer totalItemCount,
            String requestedBy,
            String reason,
            LocalDateTime createdAt,
            LocalDateTime updatedAt) {
    }

    /**
     * Backfill item read model.
     *
     * @param id item id
     * @param backfillBatchId owning batch id
     * @param bizDate business date covered by this item
     * @param flowPlanVersionId published FlowPlanVersion used for expansion
     * @param scheduleNodeId ScheduleNode generated for this item
     * @param nodeCode generated node code
     * @param targetAssetKey target asset expected to advance after delivery
     * @param workflowInstanceId generated workflow wrapper id
     * @param taskInstanceId generated task intent id
     * @param status scheduler-side item status
     * @param createdAt record creation timestamp
     * @param updatedAt record update timestamp
     */
    public record BackfillItemView(
            Long id,
            Long backfillBatchId,
            LocalDate bizDate,
            Long flowPlanVersionId,
            Long scheduleNodeId,
            String nodeCode,
            String targetAssetKey,
            Long workflowInstanceId,
            Long taskInstanceId,
            String status,
            LocalDateTime createdAt,
            LocalDateTime updatedAt) {
    }

    /**
     * Find a backfill batch by the action key that requested it.
     *
     * @param actionKey scheduling action key
     * @return backfill batch view when present
     */
    public Optional<BackfillBatchView> findBatchByActionKey(String actionKey) {
        if (isBlank(actionKey)) {
            return Optional.empty();
        }
        return backfillBatchRepository.findByActionKey(actionKey.trim())
                .map(this::toBatchView);
    }

    /**
     * List node/date items expanded for a backfill batch.
     *
     * @param backfillBatchId owning batch id
     * @return ordered backfill item views
     */
    public List<BackfillItemView> findBatchItems(Long backfillBatchId) {
        if (backfillBatchId == null || backfillBatchId <= 0) {
            return List.of();
        }
        return backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(backfillBatchId)
                .stream()
                .map(this::toItemView)
                .toList();
    }

    /**
     * Convert a persisted batch into its read model.
     *
     * @param batch persisted backfill batch
     * @return backfill batch view
     */
    private BackfillBatchView toBatchView(BackfillBatch batch) {
        return new BackfillBatchView(
                batch.getId(),
                batch.getBatchKey(),
                batch.getActionKey(),
                batch.getFlowPlanVersionId(),
                batch.getWorkflowCode(),
                batch.getWorkflowVersion(),
                batch.getScopeType(),
                batch.getEntryNodeCodes(),
                batch.getSelectedNodeCodes(),
                batch.getStartScheduleNodeId(),
                batch.getStartNodeCode(),
                batch.getBizDateStart(),
                batch.getBizDateEnd(),
                batch.getCascadePolicy(),
                batch.getProgressionMode(),
                batch.getMaxActiveDates(),
                batch.getSkipPolicy(),
                batch.getSkippedDateCount(),
                batch.getSkipEvidenceJson(),
                batch.getSourceBackfillBatchId(),
                batch.getRecoveryAttempt(),
                batch.getRecoveryStrategy(),
                batch.getStatus(),
                batch.getProducedWorkflowCount(),
                batch.getTotalItemCount(),
                batch.getRequestedBy(),
                batch.getReason(),
                batch.getCreatedAt(),
                batch.getUpdatedAt());
    }

    /**
     * Convert a persisted item into its read model.
     *
     * @param item persisted backfill item
     * @return backfill item view
     */
    private BackfillItemView toItemView(BackfillItem item) {
        return new BackfillItemView(
                item.getId(),
                item.getBackfillBatchId(),
                item.getBizDate(),
                item.getFlowPlanVersionId(),
                item.getScheduleNodeId(),
                item.getNodeCode(),
                item.getTargetAssetKey(),
                item.getWorkflowInstanceId(),
                item.getTaskInstanceId(),
                item.getStatus(),
                item.getCreatedAt(),
                item.getUpdatedAt());
    }

    /**
     * Check whether a key has no meaningful text.
     *
     * @param value source value
     * @return true when the value is null or blank
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
