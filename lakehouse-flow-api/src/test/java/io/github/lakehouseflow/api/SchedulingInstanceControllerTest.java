package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.TaskInstanceResponse;
import io.github.lakehouseflow.api.dto.WorkflowInstanceResponse;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import io.github.lakehouseflow.service.TaskInstanceService;
import io.github.lakehouseflow.service.WorkflowInstanceService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Tests SchedulingInstanceController read-only scheduling and snapshot evidence APIs.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingInstanceControllerTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 0, 0);
    private static final LocalDateTime NOW = LocalDateTime.of(2026, 9, 12, 10, 0);

    @Mock
    private WorkflowInstanceService workflowInstanceService;

    @Mock
    private TaskInstanceService taskInstanceService;

    /**
     * Verify workflow instance lookup returns a read-only scheduling view.
     */
    @Test
    void getWorkflowInstanceReturnsWorkflowWhenPresent() {
        SchedulingInstanceController controller = new SchedulingInstanceController(
                workflowInstanceService,
                taskInstanceService);
        when(workflowInstanceService.getInstance(11L)).thenReturn(Optional.of(workflowInstance()));

        ResponseEntity<WorkflowInstanceResponse> response = controller.getWorkflowInstance(11L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("orders_flow", response.getBody().workflowCode());
        assertEquals(51L, response.getBody().flowPlanVersionId());
    }

    /**
     * Verify workflow instance lookup returns 404 when absent.
     */
    @Test
    void getWorkflowInstanceReturnsNotFoundWhenAbsent() {
        SchedulingInstanceController controller = new SchedulingInstanceController(
                workflowInstanceService,
                taskInstanceService);
        when(workflowInstanceService.getInstance(404L)).thenReturn(Optional.empty());

        ResponseEntity<WorkflowInstanceResponse> response = controller.getWorkflowInstance(404L);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    /**
     * Verify task instance lookup includes target snapshot evidence fields.
     */
    @Test
    void getTaskInstanceReturnsTaskWhenPresent() {
        SchedulingInstanceController controller = new SchedulingInstanceController(
                workflowInstanceService,
                taskInstanceService);
        when(taskInstanceService.getInstance(22L)).thenReturn(Optional.of(taskInstance()));

        ResponseEntity<TaskInstanceResponse> response = controller.getTaskInstance(22L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(51L, response.getBody().flowPlanVersionId());
        assertEquals(61L, response.getBody().scheduleNodeId());
        assertEquals("paimon.dwd.orders", response.getBody().targetAssetKey());
        assertEquals("101", response.getBody().observedSnapshotId());
        assertEquals("HEALTHY", response.getBody().sourceHealth());
    }

    /**
     * Verify task instance lookup returns 404 when absent.
     */
    @Test
    void getTaskInstanceReturnsNotFoundWhenAbsent() {
        SchedulingInstanceController controller = new SchedulingInstanceController(
                workflowInstanceService,
                taskInstanceService);
        when(taskInstanceService.getInstance(404L)).thenReturn(Optional.empty());

        ResponseEntity<TaskInstanceResponse> response = controller.getTaskInstance(404L);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    /**
     * Build a workflow scheduling instance fixture.
     */
    private WorkflowInstance workflowInstance() {
        return WorkflowInstance.builder()
                .id(11L)
                .instanceKey("orders_flow:2:2026-09-12:SNAPSHOT:event-1")
                .workflowCode("orders_flow")
                .workflowVersion(2)
                .flowPlanVersionId(51L)
                .bizDate(BIZ_DATE)
                .triggerType("SNAPSHOT")
                .triggerEventId("event-1")
                .triggerReason("snapshot advanced")
                .state(SchedulingStates.SCHEDULED)
                .scheduledAt(NOW)
                .lastSnapshotCheckAt(NOW)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }

    /**
     * Build a task scheduling instance fixture with snapshot evidence.
     */
    private TaskInstance taskInstance() {
        return TaskInstance.builder()
                .id(22L)
                .instanceKey("11:dwd_orders")
                .workflowInstanceId(11L)
                .taskCode("dwd_orders")
                .taskVersion(2)
                .flowPlanVersionId(51L)
                .scheduleNodeId(61L)
                .bizDate(BIZ_DATE)
                .state(SchedulingStates.SCHEDULED)
                .waitingReason(null)
                .targetAssetKey("paimon.dwd.orders")
                .baselineSnapshotId("100")
                .observedSnapshotId("101")
                .sourceHealth("HEALTHY")
                .sourceHealthDetail("source caught up")
                .sourceEvidenceCheckedAt(NOW)
                .scheduledAt(NOW)
                .lastSnapshotCheckAt(NOW)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }
}
