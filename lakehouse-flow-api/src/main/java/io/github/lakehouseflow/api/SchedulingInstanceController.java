package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.TaskInstanceResponse;
import io.github.lakehouseflow.api.dto.WorkflowInstanceResponse;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import io.github.lakehouseflow.service.TaskInstanceService;
import io.github.lakehouseflow.service.WorkflowInstanceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for querying scheduler-side instances and snapshot evidence.
 *
 * These endpoints are read-only views over scheduling records. They do not
 * expose executor attempts or runtime task status.
 */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class SchedulingInstanceController {

    private final WorkflowInstanceService workflowInstanceService;
    private final TaskInstanceService taskInstanceService;

    /**
     * Read one workflow scheduling instance.
     *
     * @param workflowInstanceId workflow instance id
     * @return workflow instance or 404 when absent
     */
    @GetMapping("/workflow-instances/{workflowInstanceId}")
    public ResponseEntity<WorkflowInstanceResponse> getWorkflowInstance(@PathVariable Long workflowInstanceId) {
        return workflowInstanceService.getInstance(workflowInstanceId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Read one task scheduling instance with snapshot evidence.
     *
     * @param taskInstanceId task instance id
     * @return task instance or 404 when absent
     */
    @GetMapping("/task-instances/{taskInstanceId}")
    public ResponseEntity<TaskInstanceResponse> getTaskInstance(@PathVariable Long taskInstanceId) {
        return taskInstanceService.getInstance(taskInstanceId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Convert a workflow instance into an API response.
     *
     * @param workflow workflow scheduling instance
     * @return API response
     */
    private WorkflowInstanceResponse toResponse(WorkflowInstance workflow) {
        return new WorkflowInstanceResponse(
                workflow.getId(),
                workflow.getInstanceKey(),
                workflow.getWorkflowCode(),
                workflow.getWorkflowVersion(),
                workflow.getFlowPlanVersionId(),
                workflow.getBizDate(),
                workflow.getTriggerType(),
                workflow.getTriggerEventId(),
                workflow.getTriggerReason(),
                workflow.getState(),
                workflow.getScheduledAt(),
                workflow.getLastSnapshotCheckAt(),
                workflow.getCreatedAt(),
                workflow.getUpdatedAt());
    }

    /**
     * Convert a task instance into an API response.
     *
     * @param task task scheduling instance
     * @return API response
     */
    private TaskInstanceResponse toResponse(TaskInstance task) {
        return new TaskInstanceResponse(
                task.getId(),
                task.getInstanceKey(),
                task.getWorkflowInstanceId(),
                task.getTaskCode(),
                task.getTaskVersion(),
                task.getFlowPlanVersionId(),
                task.getScheduleNodeId(),
                task.getBizDate(),
                task.getState(),
                task.getWaitingReason(),
                task.getTargetAssetKey(),
                task.getBaselineSnapshotId(),
                task.getObservedSnapshotId(),
                task.getScheduledAt(),
                task.getLastSnapshotCheckAt(),
                task.getSourceHealth(),
                task.getSourceHealthDetail(),
                task.getSourceEvidenceCheckedAt(),
                task.getCreatedAt(),
                task.getUpdatedAt());
    }
}
