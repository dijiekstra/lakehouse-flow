package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests scheduler-owned publication and read-only scheduling-intent audit.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingIntentServiceTest {

    private static final String TARGET_ASSET = "paimon.prod.dwd_orders.dt=2026-09-13";

    @Mock
    private TaskInstanceService taskInstanceService;

    @Mock
    private WorkflowInstanceService workflowInstanceService;

    @Mock
    private SnapshotProgressService snapshotProgressService;

    @Mock
    private BackfillBatchRepository backfillBatchRepository;

    @Mock
    private BackfillItemRepository backfillItemRepository;

    @Mock
    private TaskInstanceRepository taskInstanceRepository;

    @Mock
    private WorkflowInstanceRepository workflowInstanceRepository;

    @Mock
    private FlowPlanVersionRepository flowPlanVersionRepository;

    @Mock
    private SchedulingIntentRepository schedulingIntentRepository;

    @Mock
    private SchedulingIntentDeliveryRepository schedulingIntentDeliveryRepository;

    @Mock
    private SchedulingTargetAdmissionService schedulingTargetAdmissionService;

    @Mock
    private FlowPlanPolicyService flowPlanPolicyService;

    @InjectMocks
    private SchedulingIntentService schedulingIntentService;

    /** Configure generated identifiers for repository saves. */
    @BeforeEach
    void setUpGeneratedIds() {
        lenient().when(flowPlanVersionRepository.findByIdForUpdate(5L))
                .thenReturn(Optional.of(FlowPlanVersion.builder().id(5L).build()));
        lenient().when(flowPlanPolicyService.resolve(
                        any(TaskInstance.class),
                        any(Duration.class),
                        any(Duration.class)))
                .thenReturn(new FlowPlanPolicyService.EffectivePolicy(
                        Duration.ofHours(1),
                        Duration.ofHours(1),
                        "PARALLEL",
                        Integer.MAX_VALUE));
        lenient().when(schedulingIntentRepository.save(any(SchedulingIntent.class))).thenAnswer(invocation -> {
            SchedulingIntent intent = invocation.getArgument(0);
            intent.setId(101L);
            return intent;
        });
        lenient().when(schedulingIntentDeliveryRepository.save(any(SchedulingIntentDelivery.class))).thenAnswer(invocation -> {
            SchedulingIntentDelivery delivery = invocation.getArgument(0);
            delivery.setId(201L);
            return delivery;
        });
        lenient().when(schedulingTargetAdmissionService.acquire(
                        anyString(),
                        any(LocalDate.class),
                        anyLong(),
                        anyString(),
                        anyString(),
                        any(Duration.class)))
                .thenAnswer(invocation -> new SchedulingTargetAdmissionService.AdmissionDecision(
                        true,
                        false,
                        invocation.getArgument(2),
                        invocation.getArgument(3),
                        LocalDateTime.now().plusHours(1)));
    }

    /** Verify an internal scan publishes a bounded set of eligible ready tasks. */
    @Test
    void publishReadyTaskIntentsFiltersUnavailableAndMissingTargetTasks() {
        TaskInstance publishable = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        TaskInstance unavailable = task(23L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        TaskInstance missingTarget = task(24L, SchedulingStates.READY_TO_SCHEDULE, null);
        when(taskInstanceService.findSchedulableTasks())
                .thenReturn(List.of(publishable, unavailable, missingTarget));
        when(backfillItemRepository.findUnavailableTaskInstanceIds(List.of(22L, 23L),
                BackfillItemStatuses.INTENT_READY, BackfillBatchStatuses.EXPANDED))
                .thenReturn(Set.of(23L));
        prepareOrdinaryPublication(publishable, workflow(SchedulingStates.READY_TO_SCHEDULE));

        List<SchedulingIntentService.TaskSchedulingIntent> result =
                schedulingIntentService.publishReadyTaskIntents(10);

        assertEquals(1, result.size());
        assertEquals(22L, result.get(0).taskInstanceId());
        verify(taskInstanceRepository, never()).findById(23L);
        verify(taskInstanceRepository, never()).findById(24L);
    }

    /** Verify an empty ready set produces no outbox writes. */
    @Test
    void publishReadyTaskIntentsReturnsEmptyWhenNothingIsReady() {
        when(taskInstanceService.findSchedulableTasks()).thenReturn(List.of());

        assertTrue(schedulingIntentService.publishReadyTaskIntents(null).isEmpty());

        verify(schedulingIntentRepository, never()).save(any());
    }

    /** Verify internal scan limits remain bounded. */
    @Test
    void publishReadyTaskIntentsRejectsInvalidBatchSize() {
        assertThrows(IllegalArgumentException.class,
                () -> schedulingIntentService.publishReadyTaskIntents(0));
        assertThrows(IllegalArgumentException.class,
                () -> schedulingIntentService.publishReadyTaskIntents(501));
    }

    /** Verify publication freezes baseline before creating database outbox evidence. */
    @Test
    void publishTaskIntentCreatesImmutableOutboxAndSnapshotBaseline() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        WorkflowInstance workflow = workflow(SchedulingStates.READY_TO_SCHEDULE);
        prepareOrdinaryPublication(task, workflow);
        when(snapshotProgressService.findLatestSnapshotId(TARGET_ASSET)).thenReturn(Optional.of("100"));

        SchedulingIntentService.TaskSchedulingIntent result =
                schedulingIntentService.publishTaskIntent(22L);

        assertEquals(101L, result.intentId());
        assertEquals("task-instance:22", result.intentKey());
        assertEquals(SnapshotEvidenceContract.CONTRACT_VERSION, result.contractVersion());
        assertEquals("SNAPSHOT", result.triggerType());
        assertEquals("100", result.baselineSnapshotId());
        assertEquals(SchedulingIntentDeliveryChannels.DATABASE_TABLE, result.deliveryChannel());
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHED, result.deliveryStatus());
        verify(taskInstanceService).markScheduled(22L, TARGET_ASSET, "100");
        verify(workflowInstanceService).markScheduled(11L);

        InOrder publicationOrder = inOrder(
                schedulingTargetAdmissionService,
                snapshotProgressService,
                schedulingIntentRepository,
                taskInstanceService);
        publicationOrder.verify(schedulingTargetAdmissionService).acquire(
                TARGET_ASSET,
                LocalDate.of(2026, 9, 13),
                22L,
                "task-instance:22",
                "SNAPSHOT",
                Duration.ofHours(1));
        publicationOrder.verify(snapshotProgressService).findLatestSnapshotId(TARGET_ASSET);
        publicationOrder.verify(schedulingIntentRepository).save(any(SchedulingIntent.class));
        publicationOrder.verify(taskInstanceService).markScheduled(22L, TARGET_ASSET, "100");

        ArgumentCaptor<SchedulingIntent> intentCaptor = ArgumentCaptor.forClass(SchedulingIntent.class);
        verify(schedulingIntentRepository).save(intentCaptor.capture());
        assertEquals(TARGET_ASSET, intentCaptor.getValue().getTargetAssetKey());
        assertEquals("100", intentCaptor.getValue().getBaselineSnapshotId());
        assertEquals(SnapshotEvidenceContract.CONTRACT_VERSION,
                intentCaptor.getValue().getContractVersion());
        assertRequiredSnapshotProperties(
                intentCaptor.getValue().getInstructionPayloadJson(),
                "task-instance:22");
        assertPublicationAdmission(
                intentCaptor.getValue().getInstructionPayloadJson(),
                "task-instance:22");
    }

    /** Verify an HTTP route creates pending transport evidence for the internal publisher. */
    @Test
    void publishTaskIntentCreatesPendingExternalDelivery() {
        ReflectionTestUtils.setField(schedulingIntentService, "deliveryChannel", "http");
        ReflectionTestUtils.setField(
                schedulingIntentService,
                "deliveryDestination",
                "https://downstream.example/intents");
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        prepareOrdinaryPublication(task, workflow(SchedulingStates.READY_TO_SCHEDULE));

        SchedulingIntentService.TaskSchedulingIntent result = schedulingIntentService.publishTaskIntent(22L);

        assertEquals(SchedulingIntentDeliveryChannels.HTTP, result.deliveryChannel());
        assertEquals("https://downstream.example/intents", result.deliveryDestination());
        assertEquals(SchedulingIntentDeliveryStatuses.PENDING, result.deliveryStatus());
        assertEquals(0, result.deliveryAttemptCount());
        assertNull(result.publishedAt());
        ArgumentCaptor<SchedulingIntentDelivery> deliveryCaptor =
                ArgumentCaptor.forClass(SchedulingIntentDelivery.class);
        verify(schedulingIntentDeliveryRepository).save(deliveryCaptor.capture());
        assertNotNull(deliveryCaptor.getValue().getDeliverBefore());
    }

    /** Verify frozen version policy controls confirmation evidence and target lease. */
    @Test
    void publishTaskIntentAppliesFrozenRuntimePolicy() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        prepareOrdinaryPublication(task, workflow(SchedulingStates.READY_TO_SCHEDULE));
        when(flowPlanPolicyService.resolve(task, Duration.ofHours(1), Duration.ofHours(1)))
                .thenReturn(new FlowPlanPolicyService.EffectivePolicy(
                        Duration.ofMinutes(30),
                        Duration.ofHours(2),
                        "PARALLEL",
                        3));

        schedulingIntentService.publishTaskIntent(22L);

        verify(schedulingTargetAdmissionService).acquire(
                TARGET_ASSET,
                LocalDate.of(2026, 9, 13),
                22L,
                "task-instance:22",
                "SNAPSHOT",
                Duration.ofHours(2));
        ArgumentCaptor<SchedulingIntent> intentCaptor = ArgumentCaptor.forClass(SchedulingIntent.class);
        verify(schedulingIntentRepository).save(intentCaptor.capture());
        Map<String, Object> payload = intentCaptor.getValue().getInstructionPayloadJson();
        assertEffectivePolicy(payload, "PT30M", "PARALLEL", 3);
    }

    /** Verify a first-ever target snapshot may be confirmed from a null baseline later. */
    @Test
    void publishTaskIntentAllowsManagedAssetWithoutExistingSnapshot() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        prepareOrdinaryPublication(task, workflow(SchedulingStates.SCHEDULED));
        when(snapshotProgressService.findLatestSnapshotId(TARGET_ASSET)).thenReturn(Optional.empty());

        SchedulingIntentService.TaskSchedulingIntent result =
                schedulingIntentService.publishTaskIntent(22L);

        assertNull(result.baselineSnapshotId());
        verify(workflowInstanceService, never()).markScheduled(11L);
    }

    /** Verify repeated publication returns the immutable existing intent. */
    @Test
    void publishTaskIntentIsIdempotent() {
        TaskInstance scheduled = task(22L, SchedulingStates.SCHEDULED, TARGET_ASSET);
        WorkflowInstance workflow = workflow(SchedulingStates.SCHEDULED);
        SchedulingIntent intent = intent();
        SchedulingIntentDelivery delivery = delivery();
        prepareLockedContext(scheduled, workflow);
        when(schedulingIntentRepository.findByTaskInstanceId(22L)).thenReturn(Optional.of(intent));
        when(schedulingIntentDeliveryRepository.findBySchedulingIntentId(101L))
                .thenReturn(Optional.of(delivery));

        SchedulingIntentService.TaskSchedulingIntent result =
                schedulingIntentService.publishTaskIntent(22L);

        assertEquals(101L, result.intentId());
        verify(snapshotProgressService, never()).findLatestSnapshotId(any());
        verify(taskInstanceService, never()).markScheduled(any(), any(), any());
    }

    /** Verify cancelled workflows cannot publish new instructions. */
    @Test
    void publishTaskIntentRejectsCancelledWorkflow() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        prepareOrdinaryPublication(task, workflow(SchedulingStates.CANCELLED));

        assertThrows(IllegalStateException.class,
                () -> schedulingIntentService.publishTaskIntent(22L));

        verify(schedulingIntentRepository, never()).save(any());
    }

    /** Verify only scheduler-ready decisions may enter the outbox. */
    @Test
    void publishTaskIntentRejectsNonReadyTask() {
        TaskInstance task = task(22L, SchedulingStates.WAITING_SNAPSHOT, TARGET_ASSET);
        prepareLockedContext(task, workflow(SchedulingStates.READY_TO_SCHEDULE));
        when(schedulingIntentRepository.findByTaskInstanceId(22L)).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class,
                () -> schedulingIntentService.publishTaskIntent(22L));
    }

    /** Verify an intent without a managed result asset fails closed. */
    @Test
    void publishTaskIntentRejectsMissingManagedTargetAsset() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, " ");
        prepareOrdinaryPublication(task, workflow(SchedulingStates.READY_TO_SCHEDULE));

        assertThrows(IllegalStateException.class,
                () -> schedulingIntentService.publishTaskIntent(22L));

        verify(snapshotProgressService, never()).findLatestSnapshotId(any());
    }

    /** Verify a live target-date holder blocks baseline capture and outbox creation. */
    @Test
    void publishTaskIntentRejectsActiveTargetDateConflict() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        prepareOrdinaryPublication(task, workflow(SchedulingStates.READY_TO_SCHEDULE));
        when(schedulingTargetAdmissionService.acquire(
                TARGET_ASSET,
                LocalDate.of(2026, 9, 13),
                22L,
                "task-instance:22",
                "SNAPSHOT",
                Duration.ofHours(1)))
                .thenReturn(new SchedulingTargetAdmissionService.AdmissionDecision(
                        false,
                        false,
                        21L,
                        "task-instance:21",
                        LocalDateTime.now().plusMinutes(30)));

        assertThrows(IllegalStateException.class,
                () -> schedulingIntentService.publishTaskIntent(22L));

        verify(snapshotProgressService, never()).findLatestSnapshotId(any());
        verify(schedulingIntentRepository, never()).save(any());
    }

    /** Verify a conflicting target does not prevent later independent tasks in the scan. */
    @Test
    void publishReadyTaskIntentsSkipsConflictAndContinuesScanning() {
        TaskInstance conflicting = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        TaskInstance publishable = task(
                23L,
                SchedulingStates.READY_TO_SCHEDULE,
                "paimon.prod.payments.dt=2026-09-13");
        when(taskInstanceService.findSchedulableTasks()).thenReturn(List.of(conflicting, publishable));
        when(backfillItemRepository.findUnavailableTaskInstanceIds(
                List.of(22L, 23L),
                BackfillItemStatuses.INTENT_READY,
                BackfillBatchStatuses.EXPANDED)).thenReturn(Set.of());
        prepareOrdinaryPublication(conflicting, workflow(SchedulingStates.READY_TO_SCHEDULE));
        prepareOrdinaryPublication(publishable, workflow(SchedulingStates.READY_TO_SCHEDULE));
        when(schedulingTargetAdmissionService.acquire(
                TARGET_ASSET,
                LocalDate.of(2026, 9, 13),
                22L,
                "task-instance:22",
                "SNAPSHOT",
                Duration.ofHours(1)))
                .thenReturn(new SchedulingTargetAdmissionService.AdmissionDecision(
                        false,
                        false,
                        21L,
                        "task-instance:21",
                        LocalDateTime.now().plusMinutes(30)));

        List<SchedulingIntentService.TaskSchedulingIntent> result =
                schedulingIntentService.publishReadyTaskIntents(1);

        assertEquals(1, result.size());
        assertEquals(23L, result.get(0).taskInstanceId());
        verify(snapshotProgressService, never()).findLatestSnapshotId(TARGET_ASSET);
        verify(snapshotProgressService).findLatestSnapshotId(publishable.getTargetAssetKey());
    }

    /** Verify a full version concurrency limit leaves the workflow ready for a later scan. */
    @Test
    void publishReadyTaskIntentsWaitsWhenVersionConcurrencyIsFull() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        when(taskInstanceService.findSchedulableTasks()).thenReturn(List.of(task));
        when(backfillItemRepository.findUnavailableTaskInstanceIds(
                List.of(22L),
                BackfillItemStatuses.INTENT_READY,
                BackfillBatchStatuses.EXPANDED)).thenReturn(Set.of());
        prepareOrdinaryPublication(task, workflow(SchedulingStates.READY_TO_SCHEDULE));
        when(flowPlanPolicyService.resolve(task, Duration.ofHours(1), Duration.ofHours(1)))
                .thenReturn(new FlowPlanPolicyService.EffectivePolicy(
                        Duration.ofHours(1),
                        Duration.ofHours(1),
                        "SERIAL_WAIT",
                        1));
        when(workflowInstanceRepository.countByFlowPlanVersionIdAndState(
                5L,
                SchedulingStates.SCHEDULED)).thenReturn(1L);

        List<SchedulingIntentService.TaskSchedulingIntent> result =
                schedulingIntentService.publishReadyTaskIntents(10);

        assertTrue(result.isEmpty());
        verify(flowPlanVersionRepository).findByIdForUpdate(5L);
        verify(schedulingTargetAdmissionService, never()).acquire(
                any(), any(), anyLong(), anyString(), anyString(), any());
        verify(snapshotProgressService, never()).findLatestSnapshotId(any());
        verify(taskInstanceService, never()).markScheduled(any(), any(), any());
    }

    /** Verify active backfill publication advances only transport audit evidence. */
    @Test
    void publishTaskIntentMarksActiveBackfillItemPublished() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        WorkflowInstance workflow = workflow(SchedulingStates.READY_TO_SCHEDULE);
        BackfillBatch batch = BackfillBatch.builder().id(31L).status(BackfillBatchStatuses.EXPANDED).build();
        BackfillItem item = BackfillItem.builder()
                .id(41L)
                .backfillBatchId(31L)
                .taskInstanceId(22L)
                .status(BackfillItemStatuses.INTENT_READY)
                .build();
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));
        when(backfillItemRepository.findBackfillBatchIdByTaskInstanceId(22L)).thenReturn(Optional.of(31L));
        when(backfillBatchRepository.findByIdForUpdate(31L)).thenReturn(Optional.of(batch));
        when(backfillItemRepository.findByTaskInstanceId(22L)).thenReturn(Optional.of(item));
        when(workflowInstanceRepository.findByIdForUpdate(11L)).thenReturn(Optional.of(workflow));
        when(taskInstanceRepository.findByIdForUpdate(22L)).thenReturn(Optional.of(task));
        when(schedulingIntentRepository.findByTaskInstanceId(22L)).thenReturn(Optional.empty());
        when(snapshotProgressService.findLatestSnapshotId(TARGET_ASSET)).thenReturn(Optional.of("100"));

        SchedulingIntentService.TaskSchedulingIntent result = schedulingIntentService.publishTaskIntent(22L);

        assertEquals(BackfillItemStatuses.INTENT_DELIVERED, item.getStatus());
        assertEquals(31L, result.backfillBatchId());
        assertEquals(41L, result.backfillItemId());
        verify(backfillItemRepository).save(item);
    }

    /** Verify paused or cancelled backfill batches cannot leak new outbox rows. */
    @Test
    void publishTaskIntentRejectsUnavailableBackfillBatch() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        BackfillBatch batch = BackfillBatch.builder().id(31L).status(BackfillBatchStatuses.PAUSED).build();
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));
        when(backfillItemRepository.findBackfillBatchIdByTaskInstanceId(22L)).thenReturn(Optional.of(31L));
        when(backfillBatchRepository.findByIdForUpdate(31L)).thenReturn(Optional.of(batch));

        assertThrows(IllegalStateException.class,
                () -> schedulingIntentService.publishTaskIntent(22L));
    }

    /** Verify a queued backfill item cannot bypass date-level admission. */
    @Test
    void publishTaskIntentRejectsUnreadyBackfillItem() {
        TaskInstance task = task(22L, SchedulingStates.READY_TO_SCHEDULE, TARGET_ASSET);
        WorkflowInstance workflow = workflow(SchedulingStates.READY_TO_SCHEDULE);
        BackfillBatch batch = BackfillBatch.builder().id(31L).status(BackfillBatchStatuses.EXPANDED).build();
        BackfillItem item = BackfillItem.builder()
                .id(41L)
                .status(BackfillItemStatuses.WAITING_CONCURRENCY)
                .build();
        when(taskInstanceRepository.findById(22L)).thenReturn(Optional.of(task));
        when(backfillItemRepository.findBackfillBatchIdByTaskInstanceId(22L)).thenReturn(Optional.of(31L));
        when(backfillBatchRepository.findByIdForUpdate(31L)).thenReturn(Optional.of(batch));
        when(backfillItemRepository.findByTaskInstanceId(22L)).thenReturn(Optional.of(item));
        when(workflowInstanceRepository.findByIdForUpdate(11L)).thenReturn(Optional.of(workflow));
        when(taskInstanceRepository.findByIdForUpdate(22L)).thenReturn(Optional.of(task));
        when(schedulingIntentRepository.findByTaskInstanceId(22L)).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class,
                () -> schedulingIntentService.publishTaskIntent(22L));
    }

    /** Verify audit lookup joins immutable intent and transport evidence. */
    @Test
    void findTaskIntentReturnsPublishedAuditView() {
        when(schedulingIntentRepository.findByTaskInstanceId(22L)).thenReturn(Optional.of(intent()));
        when(schedulingIntentDeliveryRepository.findBySchedulingIntentId(101L))
                .thenReturn(Optional.of(delivery()));

        Optional<SchedulingIntentService.TaskSchedulingIntent> result =
                schedulingIntentService.findTaskIntent(22L);

        assertTrue(result.isPresent());
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHED, result.get().deliveryStatus());
    }

    /** Verify invalid and unknown task identifiers remain absent in read-only audit. */
    @Test
    void findTaskIntentReturnsEmptyWhenNotPublished() {
        assertTrue(schedulingIntentService.findTaskIntent(null).isEmpty());
        assertTrue(schedulingIntentService.findTaskIntent(0L).isEmpty());
        when(schedulingIntentRepository.findByTaskInstanceId(22L)).thenReturn(Optional.empty());
        assertTrue(schedulingIntentService.findTaskIntent(22L).isEmpty());
    }

    /** Verify incomplete transport evidence is rejected instead of inventing delivery state. */
    @Test
    void findTaskIntentRejectsMissingDeliveryEvidence() {
        when(schedulingIntentRepository.findByTaskInstanceId(22L)).thenReturn(Optional.of(intent()));
        when(schedulingIntentDeliveryRepository.findBySchedulingIntentId(101L)).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class,
                () -> schedulingIntentService.findTaskIntent(22L));
    }

    /** Prepare one ordinary lock and publication path. */
    private void prepareOrdinaryPublication(TaskInstance task, WorkflowInstance workflow) {
        prepareLockedContext(task, workflow);
        when(backfillItemRepository.findBackfillBatchIdByTaskInstanceId(task.getId()))
                .thenReturn(Optional.empty());
        when(schedulingIntentRepository.findByTaskInstanceId(task.getId())).thenReturn(Optional.empty());
        lenient().when(snapshotProgressService.findLatestSnapshotId(task.getTargetAssetKey()))
                .thenReturn(Optional.of("100"));
    }

    /** Prepare task and workflow pessimistic lock results. */
    private void prepareLockedContext(TaskInstance task, WorkflowInstance workflow) {
        when(taskInstanceRepository.findById(task.getId())).thenReturn(Optional.of(task));
        when(workflowInstanceRepository.findByIdForUpdate(task.getWorkflowInstanceId()))
                .thenReturn(Optional.of(workflow));
        when(taskInstanceRepository.findByIdForUpdate(task.getId())).thenReturn(Optional.of(task));
    }

    /** Build one task scheduling decision for tests. */
    private TaskInstance task(Long id, String state, String targetAssetKey) {
        return TaskInstance.builder()
                .id(id)
                .instanceKey("11:node-a")
                .workflowInstanceId(11L)
                .taskCode("node-a")
                .taskVersion(3)
                .flowPlanVersionId(5L)
                .scheduleNodeId(7L)
                .bizDate(LocalDateTime.of(2026, 9, 13, 0, 0))
                .state(state)
                .targetAssetKey(targetAssetKey)
                .build();
    }

    /** Build one workflow scheduling wrapper for tests. */
    private WorkflowInstance workflow(String state) {
        return WorkflowInstance.builder()
                .id(11L)
                .workflowCode("orders-flow")
                .workflowVersion(2)
                .triggerType("SNAPSHOT")
                .triggerEventId("event-100")
                .state(state)
                .build();
    }

    /** Build one immutable intent for audit tests. */
    private SchedulingIntent intent() {
        return SchedulingIntent.builder()
                .id(101L)
                .contractVersion(SnapshotEvidenceContract.CONTRACT_VERSION)
                .intentKey("task-instance:22")
                .taskInstanceId(22L)
                .workflowInstanceId(11L)
                .triggerType("SNAPSHOT")
                .taskCode("node-a")
                .taskVersion(3)
                .flowPlanVersionId(5L)
                .scheduleNodeId(7L)
                .bizDate(LocalDateTime.of(2026, 9, 13, 0, 0))
                .targetAssetKey(TARGET_ASSET)
                .baselineSnapshotId("100")
                .instructionPayloadJson(Map.of("contractVersion", SnapshotEvidenceContract.CONTRACT_VERSION))
                .createdAt(LocalDateTime.of(2026, 9, 13, 1, 0))
                .build();
    }

    /** Verify the outbox payload tells downstream exactly how to mark its final snapshot. */
    @SuppressWarnings("unchecked")
    private void assertRequiredSnapshotProperties(Map<String, Object> payload, String intentKey) {
        Map<String, Object> snapshotEvidence = (Map<String, Object>) payload.get("snapshotEvidence");
        Map<String, String> requiredProperties =
                (Map<String, String>) snapshotEvidence.get("requiredSnapshotProperties");

        assertEquals(SnapshotEvidenceContract.CONFIRMATION_MODE, snapshotEvidence.get("mode"));
        assertEquals(SnapshotEvidenceContract.REQUIRED_CHANGE_TYPE,
                snapshotEvidence.get("requiredSnapshotChangeType"));
        assertEquals(intentKey, requiredProperties.get(SnapshotEvidenceContract.INTENT_KEY_PROPERTY));
        assertEquals(TARGET_ASSET,
                requiredProperties.get(SnapshotEvidenceContract.TARGET_ASSET_PROPERTY));
        assertEquals("2026-09-13",
                requiredProperties.get(SnapshotEvidenceContract.BIZ_DATE_PROPERTY));
        assertEquals(SnapshotEvidenceContract.FINAL_VALUE,
                requiredProperties.get(SnapshotEvidenceContract.FINAL_PROPERTY));
    }

    /** Verify downstream receives the target-date lease needed to reject stale starts. */
    @SuppressWarnings("unchecked")
    private void assertPublicationAdmission(Map<String, Object> payload, String intentKey) {
        Map<String, Object> admission = (Map<String, Object>) payload.get("publicationAdmission");

        assertEquals(TARGET_ASSET, admission.get("targetAssetKey"));
        assertEquals("2026-09-13", admission.get("bizDate"));
        assertEquals(intentKey, admission.get("holderIntentKey"));
        assertTrue(admission.get("leaseExpiresAt") instanceof String);
    }

    /** Verify immutable intent payload contains effective scheduler policy evidence. */
    @SuppressWarnings("unchecked")
    private void assertEffectivePolicy(
            Map<String, Object> payload,
            String confirmationTimeout,
            String concurrencyMode,
            Integer maxActiveInstances) {
        Map<String, Object> snapshotEvidence = (Map<String, Object>) payload.get("snapshotEvidence");
        Map<String, Object> schedulingPolicy = (Map<String, Object>) payload.get("schedulingPolicy");

        assertEquals(confirmationTimeout, snapshotEvidence.get("confirmationTimeout"));
        assertEquals(concurrencyMode, schedulingPolicy.get("concurrencyMode"));
        assertEquals(maxActiveInstances, schedulingPolicy.get("maxActiveInstances"));
    }

    /** Build one database-outbox delivery record for audit tests. */
    private SchedulingIntentDelivery delivery() {
        return SchedulingIntentDelivery.builder()
                .id(201L)
                .schedulingIntentId(101L)
                .channel(SchedulingIntentDeliveryChannels.DATABASE_TABLE)
                .destination("scheduling_intent")
                .status(SchedulingIntentDeliveryStatuses.PUBLISHED)
                .publishedAt(LocalDateTime.of(2026, 9, 13, 1, 0))
                .build();
    }
}
