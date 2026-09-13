package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.ActionRequests;
import io.github.lakehouseflow.api.dto.SchedulingActionResponse;
import io.github.lakehouseflow.common.BackfillRecoveryStrategies;
import io.github.lakehouseflow.common.SchedulingActionStatuses;
import io.github.lakehouseflow.common.SchedulingActionTypes;
import io.github.lakehouseflow.model.SchedulingActionResult;
import io.github.lakehouseflow.service.SchedulingActionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests SchedulingActionController mapping for supported scheduler-side actions.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingActionControllerTest {

    @Mock
    private SchedulingActionService schedulingActionService;

    /**
     * Verify workflow rerun requests map to workflow rerun action service.
     */
    @Test
    void rerunWorkflowMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.rerunWorkflowInstance(11L, "rerun-1", "alice", "manual rerun"))
                .thenReturn(result(SchedulingActionTypes.RERUN_WORKFLOW, List.of(12L), null));

        SchedulingActionResponse response = controller.rerunWorkflow(new ActionRequests.RerunWorkflowRequest(
                11L,
                "rerun-1",
                "alice",
                "manual rerun"));

        assertEquals(SchedulingActionTypes.RERUN_WORKFLOW, response.actionType());
        assertEquals(List.of(12L), response.workflowInstanceIds());
    }

    /**
     * Verify task rerun requests map to task-level rerun action service.
     */
    @Test
    void rerunTaskMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.rerunTaskInstance(22L, "rerun-task-1", "alice", "rerun node"))
                .thenReturn(result(SchedulingActionTypes.RERUN_TASK, List.of(12L), 23L));

        SchedulingActionResponse response = controller.rerunTask(new ActionRequests.TaskInstanceActionRequest(
                22L,
                "rerun-task-1",
                "alice",
                "rerun node"));

        assertEquals(SchedulingActionTypes.RERUN_TASK, response.actionType());
        assertEquals(23L, response.taskInstanceId());
    }

    /**
     * Verify FlowPlan node rerun requests map to node-level rerun action service.
     */
    @Test
    void rerunNodeMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        LocalDate bizDate = LocalDate.of(2026, 9, 12);
        when(schedulingActionService.rerunScheduleNode(
                51L,
                "node.dwd_orders",
                bizDate,
                "rerun-node-1",
                "alice",
                "rerun node"))
                .thenReturn(result(SchedulingActionTypes.RERUN_TASK, List.of(31L), 32L));

        SchedulingActionResponse response = controller.rerunNode(new ActionRequests.RerunNodeRequest(
                51L,
                "node.dwd_orders",
                bizDate,
                "rerun-node-1",
                "alice",
                "rerun node"));

        verify(schedulingActionService).rerunScheduleNode(
                51L,
                "node.dwd_orders",
                bizDate,
                "rerun-node-1",
                "alice",
                "rerun node");
        assertEquals(SchedulingActionTypes.RERUN_TASK, response.actionType());
        assertEquals(List.of(31L), response.workflowInstanceIds());
        assertEquals(32L, response.taskInstanceId());
    }

    /**
     * Verify backfill requests map to date range scheduling decisions.
     */
    @Test
    void backfillWorkflowMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        LocalDate start = LocalDate.of(2026, 9, 10);
        LocalDate end = LocalDate.of(2026, 9, 12);
        when(schedulingActionService.backfillWorkflow(
                "orders_flow",
                2,
                start,
                end,
                "SERIAL",
                null,
                "SKIP_FULLY_CONFIRMED_DATES",
                "backfill-1",
                "alice",
                "repair"))
                .thenReturn(result(SchedulingActionTypes.BACKFILL_WORKFLOW, List.of(31L, 32L, 33L), null));

        SchedulingActionResponse response = controller.backfillWorkflow(new ActionRequests.BackfillWorkflowRequest(
                "orders_flow",
                2,
                start,
                end,
                "SERIAL",
                null,
                "SKIP_FULLY_CONFIRMED_DATES",
                "backfill-1",
                "alice",
                "repair"));

        verify(schedulingActionService).backfillWorkflow(
                "orders_flow",
                2,
                start,
                end,
                "SERIAL",
                null,
                "SKIP_FULLY_CONFIRMED_DATES",
                "backfill-1",
                "alice",
                "repair");
        assertEquals(3, response.workflowInstanceIds().size());
    }

    /**
     * Verify node backfill requests map to FlowPlan node backfill action service.
     */
    @Test
    void backfillNodeMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        LocalDate start = LocalDate.of(2026, 9, 10);
        LocalDate end = LocalDate.of(2026, 9, 12);
        when(schedulingActionService.backfillScheduleNode(
                51L,
                "node.dwd_orders",
                start,
                end,
                "DIRECT_DOWNSTREAM",
                "PARALLEL_WITH_LIMIT",
                2,
                "SKIP_FULLY_CONFIRMED_DATES",
                "backfill-node-1",
                "alice",
                "repair"))
                .thenReturn(result(SchedulingActionTypes.BACKFILL_NODE, List.of(31L, 32L, 33L), 41L));

        SchedulingActionResponse response = controller.backfillNode(new ActionRequests.BackfillNodeRequest(
                51L,
                "node.dwd_orders",
                start,
                end,
                "DIRECT_DOWNSTREAM",
                "PARALLEL_WITH_LIMIT",
                2,
                "SKIP_FULLY_CONFIRMED_DATES",
                "backfill-node-1",
                "alice",
                "repair"));

        verify(schedulingActionService).backfillScheduleNode(
                51L,
                "node.dwd_orders",
                start,
                end,
                "DIRECT_DOWNSTREAM",
                "PARALLEL_WITH_LIMIT",
                2,
                "SKIP_FULLY_CONFIRMED_DATES",
                "backfill-node-1",
                "alice",
                "repair");
        assertEquals(SchedulingActionTypes.BACKFILL_NODE, response.actionType());
        assertEquals(3, response.workflowInstanceIds().size());
        assertEquals(41L, response.taskInstanceId());
    }

    /**
     * Verify failed backfill recovery requests map to replacement-batch scheduling.
     */
    @Test
    void recoverBackfillMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.recoverBackfillBatch(
                81L,
                BackfillRecoveryStrategies.FAILED_NODE_CASCADE,
                "recover-backfill-1",
                "alice",
                "retry"))
                .thenReturn(result(SchedulingActionTypes.RECOVER_BACKFILL, List.of(41L, 42L), 71L));

        SchedulingActionResponse response = controller.recoverBackfill(
                new ActionRequests.RecoverBackfillRequest(
                        81L,
                        BackfillRecoveryStrategies.FAILED_NODE_CASCADE,
                        "recover-backfill-1",
                        "alice",
                        "retry"));

        assertEquals(SchedulingActionTypes.RECOVER_BACKFILL, response.actionType());
        assertEquals(List.of(41L, 42L), response.workflowInstanceIds());
        assertEquals(71L, response.taskInstanceId());
        verify(schedulingActionService).recoverBackfillBatch(
                81L,
                BackfillRecoveryStrategies.FAILED_NODE_CASCADE,
                "recover-backfill-1",
                "alice",
                "retry");
    }

    /**
     * Verify backfill pause requests map to scheduler-side delivery control.
     */
    @Test
    void pauseBackfillMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.pauseBackfillBatch(81L, "pause-backfill-1", "alice", "hold"))
                .thenReturn(result(SchedulingActionTypes.PAUSE_BACKFILL, List.of(), null));

        SchedulingActionResponse response = controller.pauseBackfill(
                new ActionRequests.BackfillBatchActionRequest(81L, "pause-backfill-1", "alice", "hold"));

        assertEquals(SchedulingActionTypes.PAUSE_BACKFILL, response.actionType());
        verify(schedulingActionService).pauseBackfillBatch(81L, "pause-backfill-1", "alice", "hold");
    }

    /**
     * Verify backfill resume requests map to scheduler-side delivery control.
     */
    @Test
    void resumeBackfillMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.resumeBackfillBatch(81L, "resume-backfill-1", "alice", "continue"))
                .thenReturn(result(SchedulingActionTypes.RESUME_BACKFILL, List.of(), null));

        SchedulingActionResponse response = controller.resumeBackfill(
                new ActionRequests.BackfillBatchActionRequest(81L, "resume-backfill-1", "alice", "continue"));

        assertEquals(SchedulingActionTypes.RESUME_BACKFILL, response.actionType());
        verify(schedulingActionService).resumeBackfillBatch(81L, "resume-backfill-1", "alice", "continue");
    }

    /**
     * Verify backfill cancel requests map to pending-intent cancellation.
     */
    @Test
    void cancelBackfillMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.cancelBackfillBatch(81L, "cancel-backfill-1", "alice", "stop"))
                .thenReturn(result(SchedulingActionTypes.CANCEL_BACKFILL, List.of(), null));

        SchedulingActionResponse response = controller.cancelBackfill(
                new ActionRequests.BackfillBatchActionRequest(81L, "cancel-backfill-1", "alice", "stop"));

        assertEquals(SchedulingActionTypes.CANCEL_BACKFILL, response.actionType());
        verify(schedulingActionService).cancelBackfillBatch(81L, "cancel-backfill-1", "alice", "stop");
    }

    /**
     * Verify workflow cancel requests update scheduling records only.
     */
    @Test
    void cancelWorkflowMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.cancelWorkflowInstance(11L, "cancel-wf-1", "alice", "stop"))
                .thenReturn(result(SchedulingActionTypes.CANCEL_WORKFLOW, List.of(11L), null));

        SchedulingActionResponse response = controller.cancelWorkflow(new ActionRequests.WorkflowInstanceActionRequest(
                11L,
                "cancel-wf-1",
                "alice",
                "stop"));

        assertEquals(SchedulingActionTypes.CANCEL_WORKFLOW, response.actionType());
        assertEquals(List.of(11L), response.workflowInstanceIds());
    }

    /**
     * Verify task cancel requests map to the task action service.
     */
    @Test
    void cancelTaskMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.cancelTaskInstance(22L, "cancel-task-1", "alice", "stop"))
                .thenReturn(result(SchedulingActionTypes.CANCEL_TASK, List.of(), 22L));

        SchedulingActionResponse response = controller.cancelTask(new ActionRequests.TaskInstanceActionRequest(
                22L,
                "cancel-task-1",
                "alice",
                "stop"));

        assertEquals(SchedulingActionTypes.CANCEL_TASK, response.actionType());
        assertEquals(22L, response.taskInstanceId());
    }

    /**
     * Verify task skip requests map to the task action service.
     */
    @Test
    void skipTaskMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.skipTaskInstance(22L, "skip-task-1", "alice", "ignore"))
                .thenReturn(result(SchedulingActionTypes.SKIP_TASK, List.of(), 22L));

        SchedulingActionResponse response = controller.skipTask(new ActionRequests.TaskInstanceActionRequest(
                22L,
                "skip-task-1",
                "alice",
                "ignore"));

        assertEquals(SchedulingActionTypes.SKIP_TASK, response.actionType());
        assertEquals(22L, response.taskInstanceId());
    }

    /**
     * Verify snapshot recheck requests do not emit new downstream intents.
     */
    @Test
    void recheckTaskSnapshotMapsRequest() {
        SchedulingActionController controller = new SchedulingActionController(schedulingActionService);
        when(schedulingActionService.recheckTaskSnapshot(22L, "recheck-1", "alice", "check progress"))
                .thenReturn(result(SchedulingActionTypes.RECHECK_SNAPSHOT, List.of(), 22L));

        SchedulingActionResponse response = controller.recheckTaskSnapshot(new ActionRequests.TaskInstanceActionRequest(
                22L,
                "recheck-1",
                "alice",
                "check progress"));

        assertEquals(SchedulingActionTypes.RECHECK_SNAPSHOT, response.actionType());
        assertEquals(SchedulingActionStatuses.APPLIED, response.status());
    }

    /**
     * Build a scheduling action result fixture.
     */
    private SchedulingActionResult result(String actionType, List<Long> workflowInstanceIds, Long taskInstanceId) {
        return new SchedulingActionResult(
                "action-1",
                actionType,
                SchedulingActionStatuses.APPLIED,
                "ok",
                workflowInstanceIds,
                taskInstanceId);
    }
}
