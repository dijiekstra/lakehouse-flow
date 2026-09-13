package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.TaskSchedulingIntentResponse;
import io.github.lakehouseflow.service.SchedulingIntentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only REST API for scheduling-intent audit.
 *
 * Publication is initiated by Lakehouse Flow's internal outbox scanner. This
 * controller cannot claim, deliver, execute, or report downstream task results.
 */
@RestController
@RequestMapping("/api/v1/scheduling-intents/tasks")
@RequiredArgsConstructor
public class SchedulingIntentController {

    private final SchedulingIntentService schedulingIntentService;

    /**
     * Read one task scheduling intent.
     *
     * @param taskInstanceId task instance id
     * @return task scheduling intent or 404 when absent
     */
    @GetMapping("/{taskInstanceId}")
    public ResponseEntity<TaskSchedulingIntentResponse> getTaskIntent(@PathVariable Long taskInstanceId) {
        return schedulingIntentService.findTaskIntent(taskInstanceId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Convert a service read model into an API response.
     *
     * @param intent service read model
     * @return API response
     */
    private TaskSchedulingIntentResponse toResponse(SchedulingIntentService.TaskSchedulingIntent intent) {
        return new TaskSchedulingIntentResponse(
                intent.intentId(),
                intent.contractVersion(),
                intent.intentKey(),
                intent.taskInstanceId(),
                intent.workflowInstanceId(),
                intent.triggerType(),
                intent.backfillBatchId(),
                intent.backfillItemId(),
                intent.taskCode(),
                intent.taskVersion(),
                intent.flowPlanVersionId(),
                intent.scheduleNodeId(),
                intent.bizDate(),
                intent.targetAssetKey(),
                intent.baselineSnapshotId(),
                intent.instructionPayload(),
                intent.deliveryChannel(),
                intent.deliveryDestination(),
                intent.deliveryStatus(),
                intent.deliveryAttemptCount(),
                intent.deliveryLastError(),
                intent.deliveryLastAttemptAt(),
                intent.deliveryNextAttemptAt(),
                intent.deliveryDeadLetteredAt(),
                intent.publishedAt(),
                intent.createdAt());
    }
}
