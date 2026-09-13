package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests WorkflowInstanceService public methods as scheduling-state operations.
 */
@ExtendWith(MockitoExtension.class)
class WorkflowInstanceServiceTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 0, 0);

    @Mock
    private WorkflowInstanceRepository workflowInstanceRepository;

    @InjectMocks
    private WorkflowInstanceService workflowInstanceService;

    /**
     * Verify workflow instance creation persists a deterministic scheduling key.
     */
    @Test
    void createInstancePersistsNewWorkflowSchedulingInstance() {
        when(workflowInstanceRepository.findByInstanceKey("flow.orders:2:2026-09-12T00:00:SNAPSHOT:event-100"))
                .thenReturn(Optional.empty());
        when(workflowInstanceRepository.save(any(WorkflowInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        WorkflowInstance result = workflowInstanceService.createInstance(
                "flow.orders",
                2,
                BIZ_DATE,
                "SNAPSHOT",
                "event-100",
                "dependency satisfied");

        assertEquals("flow.orders:2:2026-09-12T00:00:SNAPSHOT:event-100", result.getInstanceKey());
        assertEquals(SchedulingStates.CREATED, result.getState());
        assertNotNull(result.getCreatedAt());
        verify(workflowInstanceRepository).save(any(WorkflowInstance.class));
    }

    /**
     * Verify workflow instance creation can preserve the FlowPlanVersion anchor.
     */
    @Test
    void createInstanceWithFlowPlanVersionPersistsDefinitionAnchor() {
        when(workflowInstanceRepository.findByInstanceKey("flow.orders:2:2026-09-12T00:00:RERUN_TASK:rerun-node-1"))
                .thenReturn(Optional.empty());
        when(workflowInstanceRepository.save(any(WorkflowInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        WorkflowInstance result = workflowInstanceService.createInstance(
                "flow.orders",
                2,
                BIZ_DATE,
                "RERUN_TASK",
                "rerun-node-1",
                "rerun published node",
                51L);

        assertEquals(51L, result.getFlowPlanVersionId());
        assertEquals(SchedulingStates.CREATED, result.getState());
        verify(workflowInstanceRepository).save(any(WorkflowInstance.class));
    }

    /**
     * Verify duplicate workflow instance keys return the existing record.
     */
    @Test
    void createInstanceReturnsExistingInstanceForDuplicateKey() {
        WorkflowInstance existing = workflow(11L, SchedulingStates.CREATED);
        when(workflowInstanceRepository.findByInstanceKey("flow.orders:2:2026-09-12T00:00:MANUAL:manual"))
                .thenReturn(Optional.of(existing));

        WorkflowInstance result = workflowInstanceService.createInstance(
                "flow.orders",
                2,
                BIZ_DATE,
                "MANUAL",
                null,
                "manual run");

        assertSame(existing, result);
        verify(workflowInstanceRepository, never()).save(any(WorkflowInstance.class));
    }

    /**
     * Verify direct state transition updates scheduled timestamps.
     */
    @Test
    void transitionStateMovesWorkflowToScheduled() {
        WorkflowInstance workflow = workflow(11L, SchedulingStates.READY_TO_SCHEDULE);
        when(workflowInstanceRepository.findById(11L)).thenReturn(Optional.of(workflow));

        workflowInstanceService.transitionState(11L, SchedulingStates.SCHEDULED);

        assertEquals(SchedulingStates.SCHEDULED, workflow.getState());
        assertNotNull(workflow.getScheduledAt());
        verify(workflowInstanceRepository).save(workflow);
    }

    /**
     * Verify invalid transitions fail fast.
     */
    @Test
    void transitionStateRejectsInvalidTransition() {
        WorkflowInstance workflow = workflow(11L, SchedulingStates.CREATED);
        when(workflowInstanceRepository.findById(11L)).thenReturn(Optional.of(workflow));

        assertThrows(RuntimeException.class,
                () -> workflowInstanceService.transitionState(11L, SchedulingStates.SNAPSHOT_CONFIRMED));
    }

    /**
     * Verify workflow convenience transition methods delegate to the state machine.
     */
    @Test
    void convenienceMethodsTransitionWorkflowStates() {
        WorkflowInstance waiting = workflow(11L, SchedulingStates.CREATED);
        WorkflowInstance ready = workflow(12L, SchedulingStates.WAITING_SNAPSHOT);
        WorkflowInstance scheduled = workflow(13L, SchedulingStates.READY_TO_SCHEDULE);
        WorkflowInstance cancelled = workflow(14L, SchedulingStates.CREATED);
        WorkflowInstance confirmed = workflow(15L, SchedulingStates.SCHEDULED);
        WorkflowInstance notAdvanced = workflow(16L, SchedulingStates.SCHEDULED);

        when(workflowInstanceRepository.findById(11L)).thenReturn(Optional.of(waiting));
        when(workflowInstanceRepository.findById(12L)).thenReturn(Optional.of(ready));
        when(workflowInstanceRepository.findById(13L)).thenReturn(Optional.of(scheduled));
        when(workflowInstanceRepository.findById(14L)).thenReturn(Optional.of(cancelled));
        when(workflowInstanceRepository.findById(15L)).thenReturn(Optional.of(confirmed));
        when(workflowInstanceRepository.findById(16L)).thenReturn(Optional.of(notAdvanced));

        workflowInstanceService.markWaitingForSnapshot(11L);
        workflowInstanceService.markSchedulable(12L);
        workflowInstanceService.markScheduled(13L);
        workflowInstanceService.cancel(14L);
        workflowInstanceService.confirmSnapshotProgress(15L);
        workflowInstanceService.markSnapshotNotAdvanced(16L);

        assertEquals(SchedulingStates.WAITING_SNAPSHOT, waiting.getState());
        assertEquals(SchedulingStates.READY_TO_SCHEDULE, ready.getState());
        assertEquals(SchedulingStates.SCHEDULED, scheduled.getState());
        assertEquals(SchedulingStates.CANCELLED, cancelled.getState());
        assertEquals(SchedulingStates.SNAPSHOT_CONFIRMED, confirmed.getState());
        assertEquals(SchedulingStates.SNAPSHOT_NOT_ADVANCED, notAdvanced.getState());
    }

    /**
     * Verify instance lookup by database id.
     */
    @Test
    void getInstanceReturnsRepositoryResult() {
        WorkflowInstance workflow = workflow(11L, SchedulingStates.CREATED);
        when(workflowInstanceRepository.findById(11L)).thenReturn(Optional.of(workflow));

        Optional<WorkflowInstance> result = workflowInstanceService.getInstance(11L);

        assertTrue(result.isPresent());
        assertSame(workflow, result.get());
    }

    /**
     * Verify instance lookup by idempotency key.
     */
    @Test
    void getInstanceByKeyReturnsRepositoryResult() {
        WorkflowInstance workflow = workflow(11L, SchedulingStates.CREATED);
        when(workflowInstanceRepository.findByInstanceKey("workflow-key")).thenReturn(Optional.of(workflow));

        Optional<WorkflowInstance> result = workflowInstanceService.getInstanceByKey("workflow-key");

        assertTrue(result.isPresent());
        assertSame(workflow, result.get());
    }

    /**
     * Build a workflow fixture.
     */
    private WorkflowInstance workflow(Long id, String state) {
        return WorkflowInstance.builder()
                .id(id)
                .instanceKey("workflow-" + id)
                .workflowCode("flow.orders")
                .workflowVersion(2)
                .bizDate(BIZ_DATE)
                .triggerType("SNAPSHOT")
                .state(state)
                .build();
    }
}
