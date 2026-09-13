package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.BackfillActionEvidenceResponse;
import io.github.lakehouseflow.api.dto.SchedulingActionDetailResponse;
import io.github.lakehouseflow.api.dto.SchedulingActionSummaryResponse;
import io.github.lakehouseflow.api.dto.TaskSnapshotEvidenceResponse;
import io.github.lakehouseflow.service.SchedulingActionQueryService;
import io.github.lakehouseflow.service.SchedulingActionQueryService.ActionDetailView;
import io.github.lakehouseflow.service.SchedulingActionQueryService.ActionSummaryView;
import io.github.lakehouseflow.service.SchedulingActionQueryService.BackfillBatchEvidenceView;
import io.github.lakehouseflow.service.SchedulingActionQueryService.TaskSnapshotEvidenceView;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Read-only REST API for action summaries and target snapshot evidence.
 *
 * This controller is deliberately separate from action command endpoints. It
 * exposes scheduler decisions and observable snapshot evidence only, never an
 * executor task status.
 */
@RestController
@RequestMapping("/api/v1/scheduling-actions")
@RequiredArgsConstructor
public class SchedulingActionQueryController {

    private final SchedulingActionQueryService schedulingActionQueryService;

    /**
     * Search recent actions by Flow, immutable version, or node anchor.
     *
     * @param workflowCode workflow code filter
     * @param flowPlanVersionId FlowPlanVersion id filter
     * @param scheduleNodeId ScheduleNode id filter
     * @param limit maximum result count
     * @return newest matching action summaries
     */
    @GetMapping
    public List<SchedulingActionSummaryResponse> searchActions(
            @RequestParam(required = false) String workflowCode,
            @RequestParam(required = false) Long flowPlanVersionId,
            @RequestParam(required = false) Long scheduleNodeId,
            @RequestParam(required = false) Integer limit) {

        try {
            return schedulingActionQueryService.searchActions(
                            workflowCode,
                            flowPlanVersionId,
                            scheduleNodeId,
                            limit)
                    .stream()
                    .map(this::toSummaryResponse)
                    .toList();
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    /**
     * Read one action with related batch and target snapshot evidence.
     *
     * @param actionKey caller-provided action idempotency key
     * @return complete action response or 404 when absent
     */
    @GetMapping("/{actionKey}")
    public ResponseEntity<SchedulingActionDetailResponse> getAction(@PathVariable String actionKey) {
        return schedulingActionQueryService.findActionByKey(actionKey)
                .map(this::toDetailResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Convert a service detail view into an API response.
     *
     * @param view service detail view
     * @return API detail response
     */
    private SchedulingActionDetailResponse toDetailResponse(ActionDetailView view) {
        return new SchedulingActionDetailResponse(
                toSummaryResponse(view.action()),
                view.targetAssetKey(),
                view.targetSnapshotId(),
                view.requestPayload(),
                view.backfillBatch() == null ? null : toBatchResponse(view.backfillBatch()),
                view.snapshotEvidence().stream().map(this::toTaskEvidenceResponse).toList());
    }

    /**
     * Convert a service action summary into an API response.
     *
     * @param view service action summary
     * @return API action summary
     */
    private SchedulingActionSummaryResponse toSummaryResponse(ActionSummaryView view) {
        return new SchedulingActionSummaryResponse(
                view.id(),
                view.actionKey(),
                view.actionType(),
                view.scopeType(),
                view.status(),
                view.workflowCode(),
                view.workflowVersion(),
                view.flowPlanVersionId(),
                view.scheduleNodeId(),
                view.workflowInstanceId(),
                view.taskInstanceId(),
                view.producedWorkflowInstanceId(),
                view.producedTaskInstanceId(),
                view.bizDateStart(),
                view.bizDateEnd(),
                view.requestedBy(),
                view.reason(),
                view.resultMessage(),
                view.createdAt(),
                view.updatedAt());
    }

    /**
     * Convert service backfill evidence into an API response.
     *
     * @param view service backfill evidence
     * @return API backfill evidence
     */
    private BackfillActionEvidenceResponse toBatchResponse(BackfillBatchEvidenceView view) {
        return new BackfillActionEvidenceResponse(
                view.id(),
                view.batchKey(),
                view.actionKey(),
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
                view.totalItemCount());
    }

    /**
     * Convert service task evidence into an API response.
     *
     * @param view service task snapshot evidence
     * @return API task snapshot evidence
     */
    private TaskSnapshotEvidenceResponse toTaskEvidenceResponse(TaskSnapshotEvidenceView view) {
        return new TaskSnapshotEvidenceResponse(
                view.backfillItemId(),
                view.backfillItemStatus(),
                view.taskRecordPresent(),
                view.taskInstanceId(),
                view.workflowInstanceId(),
                view.flowPlanVersionId(),
                view.scheduleNodeId(),
                view.taskCode(),
                view.bizDate(),
                view.schedulingState(),
                view.waitingReason(),
                view.targetAssetKey(),
                view.baselineSnapshotId(),
                view.observedSnapshotId(),
                view.snapshotAdvanced(),
                view.lastSnapshotCheckAt(),
                view.scheduledAt(),
                view.schedulingIntentId(),
                view.schedulingIntentKey(),
                view.deliveryChannel(),
                view.deliveryStatus(),
                view.deliveryAttemptCount(),
                view.deliveryLastError(),
                view.deliveryNextAttemptAt(),
                view.deliveryDeadLetteredAt(),
                view.publishedAt(),
                view.updatedAt());
    }
}
