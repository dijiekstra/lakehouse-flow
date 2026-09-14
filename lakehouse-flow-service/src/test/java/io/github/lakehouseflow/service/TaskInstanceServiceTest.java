package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.TaskInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests TaskInstanceService public methods as scheduler-side state operations.
 */
@ExtendWith(MockitoExtension.class)
class TaskInstanceServiceTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 0, 0);

    @Mock
    private TaskInstanceRepository taskInstanceRepository;

    @InjectMocks
    private TaskInstanceService taskInstanceService;

    /**
     * Verify creating a task instance persists the scheduler idempotency key.
     */
    @Test
    void createInstancePersistsNewTaskSchedulingInstance() {
        when(taskInstanceRepository.findByInstanceKey("11:node.dwd_orders")).thenReturn(Optional.empty());
        when(taskInstanceRepository.save(any(TaskInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        TaskInstance result = taskInstanceService.createInstance(11L, "node.dwd_orders", 3, BIZ_DATE);

        assertEquals("11:node.dwd_orders", result.getInstanceKey());
        assertEquals(SchedulingStates.CREATED, result.getState());
        assertNotNull(result.getCreatedAt());
        verify(taskInstanceRepository).save(any(TaskInstance.class));
    }

    /**
     * Verify creating a task intent can carry the target asset without scheduling it.
     */
    @Test
    void createInstanceWithTargetAssetPersistsReadyConfirmationTarget() {
        when(taskInstanceRepository.findByInstanceKey("11:node.dwd_orders")).thenReturn(Optional.empty());
        when(taskInstanceRepository.save(any(TaskInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        TaskInstance result = taskInstanceService.createInstance(
                11L,
                "node.dwd_orders",
                3,
                BIZ_DATE,
                "paimon.prod.dwd_orders");

        assertEquals(SchedulingStates.CREATED, result.getState());
        assertEquals("paimon.prod.dwd_orders", result.getTargetAssetKey());
        assertNull(result.getBaselineSnapshotId());
    }

    /**
     * Verify task instance creation can preserve FlowPlanVersion and ScheduleNode anchors.
     */
    @Test
    void createInstanceWithDefinitionAnchorsPersistsTraceabilityFields() {
        when(taskInstanceRepository.findByInstanceKey("11:node.dwd_orders")).thenReturn(Optional.empty());
        when(taskInstanceRepository.save(any(TaskInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        TaskInstance result = taskInstanceService.createInstance(
                11L,
                "node.dwd_orders",
                3,
                BIZ_DATE,
                "paimon.prod.dwd_orders",
                51L,
                61L);

        assertEquals(51L, result.getFlowPlanVersionId());
        assertEquals(61L, result.getScheduleNodeId());
        assertEquals("paimon.prod.dwd_orders", result.getTargetAssetKey());
        verify(taskInstanceRepository).save(any(TaskInstance.class));
    }

    /**
     * Verify duplicate task instance keys return the existing record.
     */
    @Test
    void createInstanceReturnsExistingTaskForDuplicateKey() {
        TaskInstance existing = task(22L, SchedulingStates.CREATED);
        when(taskInstanceRepository.findByInstanceKey("11:node.dwd_orders")).thenReturn(Optional.of(existing));

        TaskInstance result = taskInstanceService.createInstance(11L, "node.dwd_orders", 3, BIZ_DATE);

        assertSame(existing, result);
        verify(taskInstanceRepository, never()).save(any(TaskInstance.class));
    }

    /**
     * Verify direct state transition writes the new state and waiting reason.
     */
    @Test
    void transitionStateUpdatesTaskState() {
        TaskInstance task = task(22L, SchedulingStates.CREATED);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        taskInstanceService.transitionState(22L, SchedulingStates.WAITING_SNAPSHOT, "waiting input");

        assertEquals(SchedulingStates.WAITING_SNAPSHOT, task.getState());
        assertEquals("waiting input", task.getWaitingReason());
        verify(taskInstanceRepository).save(task);
    }

    /**
     * Verify invalid task transitions fail fast.
     */
    @Test
    void transitionStateRejectsInvalidTransition() {
        TaskInstance task = task(22L, SchedulingStates.CREATED);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        assertThrows(RuntimeException.class,
                () -> taskInstanceService.transitionState(22L, SchedulingStates.SNAPSHOT_CONFIRMED, null));
    }

    /**
     * Verify convenience methods move tasks through scheduler-side states.
     */
    @Test
    void convenienceMethodsTransitionTaskStates() {
        TaskInstance waiting = task(21L, SchedulingStates.CREATED);
        TaskInstance ready = task(22L, SchedulingStates.WAITING_SNAPSHOT);
        TaskInstance scheduled = task(23L, SchedulingStates.READY_TO_SCHEDULE);
        TaskInstance skipped = task(24L, SchedulingStates.READY_TO_SCHEDULE);
        TaskInstance cancelled = task(25L, SchedulingStates.READY_TO_SCHEDULE);

        when(taskInstanceRepository.findById(21L)).thenReturn(Optional.of(waiting));
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(ready));
        when(taskInstanceRepository.findById(23L)).thenReturn(Optional.of(scheduled));
        when(taskInstanceRepository.findById(24L)).thenReturn(Optional.of(skipped));
        when(taskInstanceRepository.findById(25L)).thenReturn(Optional.of(cancelled));

        taskInstanceService.markWaitingForSnapshot(21L, "waiting upstream");
        taskInstanceService.markSchedulable(22L);
        taskInstanceService.markScheduled(23L);
        taskInstanceService.markSkipped(24L, "manual skip");
        taskInstanceService.cancel(25L, "manual cancel");

        assertEquals(SchedulingStates.WAITING_SNAPSHOT, waiting.getState());
        assertEquals(SchedulingStates.READY_TO_SCHEDULE, ready.getState());
        assertEquals(SchedulingStates.SCHEDULED, scheduled.getState());
        assertEquals(SchedulingStates.SKIPPED, skipped.getState());
        assertEquals(SchedulingStates.CANCELLED, cancelled.getState());
        assertEquals("manual skip", skipped.getWaitingReason());
        assertEquals("manual cancel", cancelled.getWaitingReason());
    }

    /** Verify an explicit action entry preserves its intentional parent-dependency bypass. */
    @Test
    void markSchedulableAsActionEntryPersistsBypassEvidence() {
        TaskInstance task = task(22L, SchedulingStates.WAITING_SNAPSHOT);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        taskInstanceService.markSchedulableAsActionEntry(22L);

        assertTrue(task.isParentDependencyBypassed());
        assertEquals(SchedulingStates.READY_TO_SCHEDULE, task.getState());
        verify(taskInstanceRepository, org.mockito.Mockito.times(2)).save(task);
    }

    /**
     * Verify markScheduled with target asset captures baseline snapshot evidence.
     */
    @Test
    void markScheduledWithBaselineRecordsTargetSnapshotEvidence() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        taskInstanceService.markScheduled(22L, "paimon.prod.dwd_orders", "100");

        assertEquals(SchedulingStates.SCHEDULED, task.getState());
        assertEquals("paimon.prod.dwd_orders", task.getTargetAssetKey());
        assertEquals("100", task.getBaselineSnapshotId());
        assertEquals("100", task.getObservedSnapshotId());
    }

    /**
     * Verify a non-terminal snapshot check only updates evidence and waiting reason.
     */
    @Test
    void recordSnapshotCheckStoresObservedSnapshotEvidence() {
        TaskInstance task = task(22L, SchedulingStates.SCHEDULED);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        taskInstanceService.recordSnapshotCheck(
                22L,
                "paimon.prod.dwd_orders",
                "100",
                "100",
                "source unavailable",
                "SOURCE_BLOCKED",
                "RETENTION_GAP",
                BIZ_DATE.plusHours(1));

        assertEquals(SchedulingStates.SCHEDULED, task.getState());
        assertEquals("100", task.getBaselineSnapshotId());
        assertEquals("100", task.getObservedSnapshotId());
        assertEquals("source unavailable", task.getWaitingReason());
        assertEquals("SOURCE_BLOCKED", task.getSourceHealth());
        assertEquals("RETENTION_GAP", task.getSourceHealthDetail());
        assertEquals(BIZ_DATE.plusHours(1), task.getSourceEvidenceCheckedAt());
        assertNotNull(task.getLastSnapshotCheckAt());
    }

    /**
     * Verify snapshot confirmation moves a scheduled task to confirmed.
     */
    @Test
    void confirmSnapshotProgressStoresEvidenceAndConfirmsTask() {
        TaskInstance task = task(22L, SchedulingStates.SCHEDULED);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        taskInstanceService.confirmSnapshotProgress(
                22L,
                "paimon.prod.dwd_orders",
                "100",
                "101");

        assertEquals(SchedulingStates.SNAPSHOT_CONFIRMED, task.getState());
        assertEquals("101", task.getObservedSnapshotId());
        assertNotNull(task.getLastSnapshotCheckAt());
    }

    /**
     * Verify missing snapshot advancement records failure evidence.
     */
    @Test
    void markSnapshotNotAdvancedStoresEvidenceAndFailureReason() {
        TaskInstance task = task(22L, SchedulingStates.SCHEDULED);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        taskInstanceService.markSnapshotNotAdvanced(
                22L,
                "paimon.prod.dwd_orders",
                "100",
                "100",
                "target did not advance",
                "HEALTHY",
                "source caught up",
                BIZ_DATE.plusHours(1));

        assertEquals(SchedulingStates.SNAPSHOT_NOT_ADVANCED, task.getState());
        assertEquals("target did not advance", task.getWaitingReason());
        assertEquals("100", task.getObservedSnapshotId());
        assertEquals("HEALTHY", task.getSourceHealth());
    }

    /**
     * Verify lookup by id delegates to repository.
     */
    @Test
    void getInstanceReturnsRepositoryResult() {
        TaskInstance task = task(22L, SchedulingStates.CREATED);
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));

        Optional<TaskInstance> result = taskInstanceService.getInstance(22L);

        assertTrue(result.isPresent());
        assertSame(task, result.get());
    }

    /**
     * Verify waiting-for-snapshot query delegates to repository.
     */
    @Test
    void findWaitingForSnapshotReturnsRepositoryResult() {
        List<TaskInstance> tasks = List.of(task(22L, SchedulingStates.WAITING_SNAPSHOT));
        when(taskInstanceRepository.findWaitingForSnapshot()).thenReturn(tasks);

        List<TaskInstance> result = taskInstanceService.findWaitingForSnapshot();

        assertSame(tasks, result);
    }

    /**
     * Verify schedulable task query uses READY_TO_SCHEDULE state.
     */
    @Test
    void findSchedulableTasksReturnsReadyTasks() {
        List<TaskInstance> tasks = List.of(task(22L, SchedulingStates.READY_TO_SCHEDULE));
        when(taskInstanceRepository.findDeliverableReadyTasks()).thenReturn(tasks);

        List<TaskInstance> result = taskInstanceService.findSchedulableTasks();

        assertSame(tasks, result);
    }

    /**
     * Build a task fixture.
     */
    private TaskInstance task(Long id, String state) {
        return TaskInstance.builder()
                .id(id)
                .instanceKey("task-" + id)
                .workflowInstanceId(11L)
                .taskCode("node.dwd_orders")
                .taskVersion(3)
                .bizDate(BIZ_DATE)
                .state(state)
                .build();
    }
}
