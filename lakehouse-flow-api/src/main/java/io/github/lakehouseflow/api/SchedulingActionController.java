package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.ActionRequests;
import io.github.lakehouseflow.api.dto.SchedulingActionResponse;
import io.github.lakehouseflow.model.SchedulingActionResult;
import io.github.lakehouseflow.service.SchedulingActionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for scheduler-side actions such as rerun, backfill, cancel, skip, and recheck.
 *
 * Actions mutate scheduling records or emit scheduling decisions only. They do
 * not execute downstream tasks.
 */
@RestController
@RequestMapping("/api/v1/scheduling-actions")
@RequiredArgsConstructor
public class SchedulingActionController {

    private final SchedulingActionService schedulingActionService;

    /**
     * Emit a rerun scheduling decision for an existing workflow instance.
     *
     * @param request rerun request
     * @return scheduling action result
     */
    @PostMapping("/rerun-workflow")
    public SchedulingActionResponse rerunWorkflow(@Valid @RequestBody ActionRequests.RerunWorkflowRequest request) {
        return toResponse(schedulingActionService.rerunWorkflowInstance(
                request.workflowInstanceId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Emit a rerun scheduling intent for an existing task instance.
     *
     * @param request task rerun request
     * @return scheduling action result
     */
    @PostMapping("/rerun-task")
    public SchedulingActionResponse rerunTask(@Valid @RequestBody ActionRequests.TaskInstanceActionRequest request) {
        return toResponse(schedulingActionService.rerunTaskInstance(
                request.taskInstanceId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Emit a rerun scheduling intent for a node in a published FlowPlan version.
     *
     * @param request node rerun request
     * @return scheduling action result
     */
    @PostMapping("/rerun-node")
    public SchedulingActionResponse rerunNode(@Valid @RequestBody ActionRequests.RerunNodeRequest request) {
        return toResponse(schedulingActionService.rerunScheduleNode(
                request.flowPlanVersionId(),
                request.nodeCode(),
                request.bizDate(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Emit backfill scheduling decisions for a workflow date range.
     *
     * @param request backfill request
     * @return scheduling action result
     */
    @PostMapping("/backfill-workflow")
    public SchedulingActionResponse backfillWorkflow(
            @Valid @RequestBody ActionRequests.BackfillWorkflowRequest request) {

        return toResponse(schedulingActionService.backfillWorkflow(
                request.workflowCode(),
                request.workflowVersion(),
                request.startBizDate(),
                request.endBizDate(),
                request.progressionMode(),
                request.maxActiveDates(),
                request.skipPolicy(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Emit node-scoped backfill scheduling intents for a FlowPlan date range.
     *
     * @param request node backfill request
     * @return scheduling action result
     */
    @PostMapping("/backfill-node")
    public SchedulingActionResponse backfillNode(@Valid @RequestBody ActionRequests.BackfillNodeRequest request) {
        return toResponse(schedulingActionService.backfillScheduleNode(
                request.flowPlanVersionId(),
                request.startNodeCode(),
                request.startBizDate(),
                request.endBizDate(),
                request.cascadePolicy(),
                request.progressionMode(),
                request.maxActiveDates(),
                request.skipPolicy(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Recover a snapshot-failed backfill by emitting a replacement batch.
     *
     * @param request failed batch recovery request
     * @return scheduling action result
     */
    @PostMapping("/recover-backfill")
    public SchedulingActionResponse recoverBackfill(
            @Valid @RequestBody ActionRequests.RecoverBackfillRequest request) {

        return toResponse(schedulingActionService.recoverBackfillBatch(
                request.backfillBatchId(),
                request.recoveryStrategy(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Pause delivery of pending scheduling intents in a backfill batch.
     *
     * @param request backfill batch action request
     * @return scheduling action result
     */
    @PostMapping("/pause-backfill")
    public SchedulingActionResponse pauseBackfill(
            @Valid @RequestBody ActionRequests.BackfillBatchActionRequest request) {

        return toResponse(schedulingActionService.pauseBackfillBatch(
                request.backfillBatchId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Resume delivery of pending scheduling intents in a paused backfill batch.
     *
     * @param request backfill batch action request
     * @return scheduling action result
     */
    @PostMapping("/resume-backfill")
    public SchedulingActionResponse resumeBackfill(
            @Valid @RequestBody ActionRequests.BackfillBatchActionRequest request) {

        return toResponse(schedulingActionService.resumeBackfillBatch(
                request.backfillBatchId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Cancel pending scheduling intents in a backfill batch.
     *
     * Already delivered intents are retained because Lakehouse Flow does not
     * control downstream execution.
     *
     * @param request backfill batch action request
     * @return scheduling action result
     */
    @PostMapping("/cancel-backfill")
    public SchedulingActionResponse cancelBackfill(
            @Valid @RequestBody ActionRequests.BackfillBatchActionRequest request) {

        return toResponse(schedulingActionService.cancelBackfillBatch(
                request.backfillBatchId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Cancel a workflow scheduling instance.
     *
     * @param request workflow cancel request
     * @return scheduling action result
     */
    @PostMapping("/cancel-workflow")
    public SchedulingActionResponse cancelWorkflow(
            @Valid @RequestBody ActionRequests.WorkflowInstanceActionRequest request) {

        return toResponse(schedulingActionService.cancelWorkflowInstance(
                request.workflowInstanceId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Cancel a task scheduling instance.
     *
     * @param request task cancel request
     * @return scheduling action result
     */
    @PostMapping("/cancel-task")
    public SchedulingActionResponse cancelTask(@Valid @RequestBody ActionRequests.TaskInstanceActionRequest request) {
        return toResponse(schedulingActionService.cancelTaskInstance(
                request.taskInstanceId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Skip a task scheduling instance.
     *
     * @param request task skip request
     * @return scheduling action result
     */
    @PostMapping("/skip-task")
    public SchedulingActionResponse skipTask(@Valid @RequestBody ActionRequests.TaskInstanceActionRequest request) {
        return toResponse(schedulingActionService.skipTaskInstance(
                request.taskInstanceId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Recheck target snapshot progress for a task scheduling instance.
     *
     * @param request snapshot recheck request
     * @return scheduling action result
     */
    @PostMapping("/recheck-task-snapshot")
    public SchedulingActionResponse recheckTaskSnapshot(
            @Valid @RequestBody ActionRequests.TaskInstanceActionRequest request) {

        return toResponse(schedulingActionService.recheckTaskSnapshot(
                request.taskInstanceId(),
                request.actionKey(),
                request.requestedBy(),
                request.reason()));
    }

    /**
     * Convert a service result into an API response.
     *
     * @param result scheduling action service result
     * @return API response
     */
    private SchedulingActionResponse toResponse(SchedulingActionResult result) {
        return new SchedulingActionResponse(
                result.actionKey(),
                result.actionType(),
                result.status(),
                result.message(),
                result.workflowInstanceIds(),
                result.taskInstanceId());
    }
}
