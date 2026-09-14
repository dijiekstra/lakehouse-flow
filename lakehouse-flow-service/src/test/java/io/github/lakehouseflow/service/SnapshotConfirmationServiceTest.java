package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SnapshotConfirmationResult;
import io.github.lakehouseflow.model.TaskInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests target-snapshot confirmation for scheduled task instances.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotConfirmationServiceTest {

    private static final Long TASK_ID = 42L;
    private static final String TARGET_ASSET_KEY = "paimon.prod.dwd_orders";
    private static final LocalDateTime SOURCE_CHECKED_AT = LocalDateTime.of(2026, 9, 13, 2, 0);

    @Mock
    private TaskInstanceRepository taskInstanceRepository;

    @Mock
    private SchedulingIntentRepository schedulingIntentRepository;

    @Mock
    private TaskInstanceService taskInstanceService;

    @Mock
    private SnapshotEvidenceService snapshotEvidenceService;

    @Mock
    private DagProgressionService dagProgressionService;

    @Mock
    private SchedulingTargetAdmissionService schedulingTargetAdmissionService;

    @Mock
    private FlowPlanPolicyService flowPlanPolicyService;

    @Mock
    private SnapshotSourceHealthService snapshotSourceHealthService;

    @Mock
    private WriterJobBindingService writerJobBindingService;

    @InjectMocks
    private SnapshotConfirmationService snapshotConfirmationService;

    /** Keep existing tests on the global fallback unless they override policy resolution. */
    @BeforeEach
    void setUpPolicyFallback() {
        lenient().when(flowPlanPolicyService.resolve(
                        any(TaskInstance.class),
                        any(Duration.class),
                        any(Duration.class)))
                .thenAnswer(invocation -> new FlowPlanPolicyService.EffectivePolicy(
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        "PARALLEL",
                        Integer.MAX_VALUE));
        lenient().when(snapshotSourceHealthService.evaluateTimeoutEvidence(
                        any(String.class),
                        any(LocalDateTime.class)))
                .thenReturn(new SnapshotSourceHealthService.SourceHealthDecision(
                        "paimon.prod.dwd_orders",
                        "HEALTHY",
                        true,
                        "source caught up",
                        SOURCE_CHECKED_AT));
    }

    /**
     * Verify task confirmation when the target snapshot advances past baseline.
     */
    @Test
    void confirmsTaskWhenTargetSnapshotAdvancedBeyondBaseline() {
        TaskInstance task = scheduledTask(LocalDateTime.now().minusMinutes(5));
        when(taskInstanceRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));
        SchedulingIntent intent = schedulingIntent();
        when(schedulingIntentRepository.findByTaskInstanceId(TASK_ID)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateIntentProgress(intent))
                .thenReturn(progressResult(true, "101", null));

        SnapshotConfirmationResult result = snapshotConfirmationService.checkTaskSnapshotProgress(
                TASK_ID,
                Duration.ofHours(1));

        assertTrue(result.snapshotAdvanced());
        assertFalse(result.confirmationExpired());
        assertEquals(SchedulingStates.SNAPSHOT_CONFIRMED, result.resultingState());
        assertEquals("101", result.observedSnapshotId());
        verify(taskInstanceService).confirmSnapshotProgress(TASK_ID, TARGET_ASSET_KEY, "100", "101");
        verify(schedulingTargetAdmissionService).release(
                TARGET_ASSET_KEY,
                LocalDateTime.of(2026, 9, 13, 0, 0).toLocalDate(),
                TASK_ID,
                "SNAPSHOT_CONFIRMED");
        verify(dagProgressionService).onSnapshotConfirmed(TASK_ID);
        verify(writerJobBindingService).releaseDataIntent(
                "writer.dwd.orders", 3L, "task-instance:42");
    }

    /** Verify a stale scanner can re-read an already confirmed task without repeating side effects. */
    @Test
    void returnsConfirmedResultIdempotentlyForStaleScanner() {
        TaskInstance task = scheduledTask(LocalDateTime.now().minusMinutes(5));
        task.setState(SchedulingStates.SNAPSHOT_CONFIRMED);
        task.setObservedSnapshotId("101");
        when(taskInstanceRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        SnapshotConfirmationResult result = snapshotConfirmationService.checkTaskSnapshotProgress(
                TASK_ID,
                Duration.ofHours(1));

        assertTrue(result.snapshotAdvanced());
        assertEquals(SchedulingStates.SNAPSHOT_CONFIRMED, result.resultingState());
        assertEquals("101", result.observedSnapshotId());
        verify(schedulingIntentRepository, never()).findByTaskInstanceId(any());
        verify(taskInstanceService, never()).confirmSnapshotProgress(any(), any(), any(), any());
        verify(dagProgressionService, never()).onSnapshotConfirmed(any());
    }

    /**
     * Verify pending snapshot checks keep the task scheduled before timeout.
     */
    @Test
    void recordsSnapshotCheckButKeepsTaskScheduledBeforeTimeout() {
        TaskInstance task = scheduledTask(LocalDateTime.now().minusMinutes(5));
        when(taskInstanceRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));
        SchedulingIntent intent = schedulingIntent();
        when(schedulingIntentRepository.findByTaskInstanceId(TASK_ID)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateIntentProgress(intent))
                .thenReturn(progressResult(false, "100", "Waiting for target snapshot > 100"));

        SnapshotConfirmationResult result = snapshotConfirmationService.checkTaskSnapshotProgress(
                TASK_ID,
                Duration.ofHours(1));

        assertFalse(result.snapshotAdvanced());
        assertFalse(result.confirmationExpired());
        assertEquals(SchedulingStates.SCHEDULED, result.resultingState());
        assertEquals("Waiting for target snapshot > 100", result.waitingReason());
        verify(taskInstanceService).recordSnapshotCheck(
                TASK_ID,
                TARGET_ASSET_KEY,
                "100",
                "100",
                "Waiting for target snapshot > 100",
                null,
                null,
                null);
        verify(taskInstanceService, never()).markSnapshotNotAdvanced(
                any(), any(), any(), any(), any(), any(), any(), any());
        verify(schedulingTargetAdmissionService, never()).release(any(), any(), any(), any());
    }

    /**
     * Verify timeout converts missing target progress into snapshot-not-advanced.
     */
    @Test
    void marksTaskNotAdvancedWhenConfirmationWindowExpired() {
        TaskInstance task = scheduledTask(LocalDateTime.now().minusHours(2));
        when(taskInstanceRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));
        SchedulingIntent intent = schedulingIntent();
        when(schedulingIntentRepository.findByTaskInstanceId(TASK_ID)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateIntentProgress(intent))
                .thenReturn(progressResult(false, "100", "Waiting for target snapshot > 100"));

        SnapshotConfirmationResult result = snapshotConfirmationService.checkTaskSnapshotProgress(
                TASK_ID,
                Duration.ofHours(1));

        assertFalse(result.snapshotAdvanced());
        assertTrue(result.confirmationExpired());
        assertEquals(SchedulingStates.SNAPSHOT_NOT_ADVANCED, result.resultingState());
        assertEquals("HEALTHY", result.sourceHealth());
        verify(taskInstanceService).markSnapshotNotAdvanced(
                TASK_ID,
                TARGET_ASSET_KEY,
                "100",
                "100",
                "Waiting for target snapshot > 100",
                "HEALTHY",
                "source caught up",
                SOURCE_CHECKED_AT);
        verify(schedulingTargetAdmissionService).release(
                TARGET_ASSET_KEY,
                LocalDateTime.of(2026, 9, 13, 0, 0).toLocalDate(),
                TASK_ID,
                "SNAPSHOT_CONFIRMATION_EXPIRED");
        verify(dagProgressionService).onSnapshotNotAdvanced(TASK_ID);
    }

    /** Verify an elapsed window remains pending when source evidence is incomplete. */
    @Test
    void keepsTaskScheduledWhenSourceCannotProveCompleteWindow() {
        TaskInstance task = scheduledTask(LocalDateTime.now().minusHours(2));
        when(taskInstanceRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));
        SchedulingIntent intent = schedulingIntent();
        when(schedulingIntentRepository.findByTaskInstanceId(TASK_ID)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateIntentProgress(intent))
                .thenReturn(progressResult(false, "100", "Waiting for target snapshot > 100"));
        when(snapshotSourceHealthService.evaluateTimeoutEvidence(
                        any(String.class),
                        any(LocalDateTime.class)))
                .thenReturn(new SnapshotSourceHealthService.SourceHealthDecision(
                        "paimon.prod.dwd_orders",
                        "SOURCE_BLOCKED",
                        false,
                        "RETENTION_GAP",
                        SOURCE_CHECKED_AT));

        SnapshotConfirmationResult result = snapshotConfirmationService.checkTaskSnapshotProgress(
                TASK_ID,
                Duration.ofHours(1));

        assertFalse(result.confirmationExpired());
        assertEquals(SchedulingStates.SCHEDULED, result.resultingState());
        assertEquals("SOURCE_BLOCKED", result.sourceHealth());
        assertTrue(result.waitingReason().contains("RETENTION_GAP"));
        verify(taskInstanceService).recordSnapshotCheck(
                TASK_ID,
                TARGET_ASSET_KEY,
                "100",
                "100",
                result.waitingReason(),
                "SOURCE_BLOCKED",
                "RETENTION_GAP",
                SOURCE_CHECKED_AT);
        verify(taskInstanceService, never()).markSnapshotNotAdvanced(
                any(), any(), any(), any(), any(), any(), any(), any());
        verify(schedulingTargetAdmissionService, never()).release(any(), any(), any(), any());
        verify(dagProgressionService, never()).onSnapshotNotAdvanced(TASK_ID);
    }

    /** Verify a node-level timeout overrides the scanner's global fallback. */
    @Test
    void marksTaskNotAdvancedUsingFrozenNodeTimeout() {
        TaskInstance task = scheduledTask(LocalDateTime.now().minusMinutes(30));
        when(taskInstanceRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));
        SchedulingIntent intent = schedulingIntent();
        when(schedulingIntentRepository.findByTaskInstanceId(TASK_ID)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateIntentProgress(intent))
                .thenReturn(progressResult(false, "100", "Waiting for attributable snapshot"));
        when(flowPlanPolicyService.resolve(task, Duration.ofHours(1), Duration.ofHours(1)))
                .thenReturn(new FlowPlanPolicyService.EffectivePolicy(
                        Duration.ofMinutes(15),
                        Duration.ofHours(1),
                        "PARALLEL",
                        Integer.MAX_VALUE));

        SnapshotConfirmationResult result = snapshotConfirmationService.checkTaskSnapshotProgress(
                TASK_ID,
                Duration.ofHours(1));

        assertTrue(result.confirmationExpired());
        verify(taskInstanceService).markSnapshotNotAdvanced(
                TASK_ID,
                TARGET_ASSET_KEY,
                "100",
                "100",
                "Waiting for attributable snapshot",
                "HEALTHY",
                "source caught up",
                SOURCE_CHECKED_AT);
    }

    /**
     * Verify batch confirmation checks every scheduled task awaiting target snapshot evidence.
     */
    @Test
    void checkScheduledTasksChecksEachScheduledTask() {
        TaskInstance task = scheduledTask(LocalDateTime.now().minusMinutes(5));
        when(taskInstanceRepository.findScheduledAwaitingSnapshotConfirmation()).thenReturn(List.of(task));
        when(taskInstanceRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));
        SchedulingIntent intent = schedulingIntent();
        when(schedulingIntentRepository.findByTaskInstanceId(TASK_ID)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateIntentProgress(intent))
                .thenReturn(progressResult(true, "101", null));

        List<SnapshotConfirmationResult> results = snapshotConfirmationService.checkScheduledTasks(Duration.ofHours(1));

        assertEquals(1, results.size());
        assertTrue(results.get(0).snapshotAdvanced());
        verify(taskInstanceService).confirmSnapshotProgress(TASK_ID, TARGET_ASSET_KEY, "100", "101");
    }

    /**
     * Build a scheduled task fixture with a fixed baseline snapshot.
     */
    private TaskInstance scheduledTask(LocalDateTime scheduledAt) {
        return TaskInstance.builder()
                .id(TASK_ID)
                .state(SchedulingStates.SCHEDULED)
                .targetAssetKey(TARGET_ASSET_KEY)
                .baselineSnapshotId("100")
                .scheduledAt(scheduledAt)
                .build();
    }

    /** Build the immutable scheduling intent used by strict snapshot attribution. */
    private SchedulingIntent schedulingIntent() {
        return SchedulingIntent.builder()
                .id(51L)
                .intentKey("task-instance:" + TASK_ID)
                .taskInstanceId(TASK_ID)
                .bizDate(LocalDateTime.of(2026, 9, 13, 0, 0))
                .targetAssetKey(TARGET_ASSET_KEY)
                .baselineSnapshotId("100")
                .writerJobKey("writer.dwd.orders")
                .writerEpoch(3L)
                .createdAt(LocalDateTime.of(2026, 9, 13, 1, 0))
                .build();
    }

    /**
     * Build an evaluation fixture returned by SnapshotProgressService.
     */
    private EvaluationResult progressResult(boolean satisfied, String snapshotId, String waitingReason) {
        return EvaluationResult.builder()
                .satisfied(satisfied)
                .assetKey(TARGET_ASSET_KEY)
                .snapshotId(snapshotId)
                .waitingReason(waitingReason)
                .build();
    }
}
