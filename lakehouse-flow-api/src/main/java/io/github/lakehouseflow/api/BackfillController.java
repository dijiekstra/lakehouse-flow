package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.BackfillBatchResponse;
import io.github.lakehouseflow.api.dto.BackfillItemResponse;
import io.github.lakehouseflow.service.BackfillQueryService;
import io.github.lakehouseflow.service.BackfillQueryService.BackfillBatchView;
import io.github.lakehouseflow.service.BackfillQueryService.BackfillItemView;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for reading scheduler-side backfill batch audit records.
 *
 * These endpoints expose expansion evidence only. They do not expose external
 * task execution progress or accept executor callbacks.
 */
@RestController
@RequestMapping("/api/v1/backfills")
@RequiredArgsConstructor
public class BackfillController {

    private final BackfillQueryService backfillQueryService;

    /**
     * Read a backfill batch by scheduling action key.
     *
     * @param actionKey scheduling action key
     * @return batch response or 404 when absent
     */
    @GetMapping("/by-action/{actionKey}")
    public ResponseEntity<BackfillBatchResponse> getBatchByActionKey(@PathVariable String actionKey) {
        return backfillQueryService.findBatchByActionKey(actionKey)
                .map(this::toBatchResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * List node/date items expanded for a backfill batch.
     *
     * @param backfillBatchId owning batch id
     * @return ordered item responses
     */
    @GetMapping("/{backfillBatchId}/items")
    public List<BackfillItemResponse> listBatchItems(@PathVariable Long backfillBatchId) {
        return backfillQueryService.findBatchItems(backfillBatchId)
                .stream()
                .map(this::toItemResponse)
                .toList();
    }

    /**
     * Convert a service batch view into an API response.
     *
     * @param view service batch view
     * @return API response
     */
    private BackfillBatchResponse toBatchResponse(BackfillBatchView view) {
        return new BackfillBatchResponse(
                view.id(),
                view.batchKey(),
                view.actionKey(),
                view.flowPlanVersionId(),
                view.workflowCode(),
                view.workflowVersion(),
                view.scopeType(),
                view.entryNodeCodes(),
                view.selectedNodeCodes(),
                view.startScheduleNodeId(),
                view.startNodeCode(),
                view.bizDateStart(),
                view.bizDateEnd(),
                view.cascadePolicy(),
                view.progressionMode(),
                view.maxActiveDates(),
                view.skipPolicy(),
                view.skippedDateCount(),
                view.skipEvidence(),
                view.sourceBackfillBatchId(),
                view.recoveryAttempt(),
                view.recoveryStrategy(),
                view.status(),
                view.producedWorkflowCount(),
                view.totalItemCount(),
                view.requestedBy(),
                view.reason(),
                view.createdAt(),
                view.updatedAt());
    }

    /**
     * Convert a service item view into an API response.
     *
     * @param view service item view
     * @return API response
     */
    private BackfillItemResponse toItemResponse(BackfillItemView view) {
        return new BackfillItemResponse(
                view.id(),
                view.backfillBatchId(),
                view.bizDate(),
                view.flowPlanVersionId(),
                view.scheduleNodeId(),
                view.nodeCode(),
                view.targetAssetKey(),
                view.workflowInstanceId(),
                view.taskInstanceId(),
                view.status(),
                view.createdAt(),
                view.updatedAt());
    }
}
