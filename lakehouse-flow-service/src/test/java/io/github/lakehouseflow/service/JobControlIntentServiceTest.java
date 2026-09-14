package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.JobControlIntentContract;
import io.github.lakehouseflow.common.JobControlOperations;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.JobControlIntentDelivery;
import io.github.lakehouseflow.model.WriterJobBinding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests independent platform writer lifecycle intents and immutable payloads.
 */
@ExtendWith(MockitoExtension.class)
class JobControlIntentServiceTest {

    @Mock
    private WriterJobBindingService writerJobBindingService;

    @Mock
    private SnapshotProgressService snapshotProgressService;

    @Mock
    private JobControlIntentRepository intentRepository;

    @Mock
    private JobControlIntentDeliveryRepository deliveryRepository;

    @InjectMocks
    private JobControlIntentService service;

    /** Configure repository-generated ids for immutable intent and delivery records. */
    @BeforeEach
    void setUpGeneratedIds() {
        org.mockito.Mockito.lenient().when(intentRepository.save(any())).thenAnswer(invocation -> {
            JobControlIntent intent = invocation.getArgument(0);
            intent.setId(11L);
            return intent;
        });
        org.mockito.Mockito.lenient().when(deliveryRepository.save(any())).thenAnswer(invocation -> {
            JobControlIntentDelivery delivery = invocation.getArgument(0);
            delivery.setId(21L);
            return delivery;
        });
    }

    /** Verify START_JOB creates epoch-one database outbox evidence without task fields. */
    @Test
    void startJobCreatesIndependentDatabaseIntent() {
        WriterJobBinding binding = binding();
        when(intentRepository.findByRequestKey("request-start-1")).thenReturn(Optional.empty());
        when(writerJobBindingService.allocateControlEpoch("writer.orders", JobControlOperations.START_JOB))
                .thenReturn(new WriterJobBindingService.ControlEpochAllocation(
                        binding, "job-control:writer.orders:1", 1L, 0L));
        when(snapshotProgressService.findLatestSnapshotId("paimon.ods.orders"))
                .thenReturn(Optional.of("811"));

        JobControlIntentService.JobControlIntentView result = service.startJob(
                "writer.orders",
                new JobControlIntentService.CreateJobControlIntentCommand(
                        "request-start-1", "operator@example.com", "initial CDC start"));

        assertEquals(JobControlIntentContract.CONTRACT_VERSION, result.contractVersion());
        assertEquals("job-control:writer.orders:1", result.intentKey());
        assertEquals(1L, result.writerEpoch());
        assertEquals(0L, result.previousWriterEpoch());
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHED, result.deliveryStatus());
        assertEquals("811", result.baselineSnapshotId());
        assertRequiredWriterProperties(result.instructionPayload(), result.intentKey(), "1");
        assertTrue(!result.instructionPayload().containsKey("identity"));
        assertTrue(!result.instructionPayload().containsKey("schedule"));
    }

    /** Verify RESTART_JOB uses the same immutable payload through an HTTP pending route. */
    @Test
    void restartJobCreatesPendingHttpDelivery() {
        ReflectionTestUtils.setField(service, "deliveryChannel", SchedulingIntentDeliveryChannels.HTTP);
        ReflectionTestUtils.setField(service, "deliveryDestination", "https://platform.example/jobs");
        WriterJobBinding binding = binding();
        binding.setCurrentWriterEpoch(7L);
        when(intentRepository.findByRequestKey("request-restart-8")).thenReturn(Optional.empty());
        when(writerJobBindingService.allocateControlEpoch("writer.orders", JobControlOperations.RESTART_JOB))
                .thenReturn(new WriterJobBindingService.ControlEpochAllocation(
                        binding, "job-control:writer.orders:8", 8L, 7L));
        when(snapshotProgressService.findLatestSnapshotId("paimon.ods.orders"))
                .thenReturn(Optional.empty());

        JobControlIntentService.JobControlIntentView result = service.restartJob(
                "writer.orders",
                new JobControlIntentService.CreateJobControlIntentCommand(
                        "request-restart-8", "alice", "checkpoint recovery"));

        assertEquals(JobControlOperations.RESTART_JOB, result.operationType());
        assertEquals(8L, result.writerEpoch());
        assertEquals(SchedulingIntentDeliveryChannels.HTTP, result.deliveryChannel());
        assertEquals(SchedulingIntentDeliveryStatuses.PENDING, result.deliveryStatus());
        assertEquals(0, result.deliveryAttemptCount());
    }

    /** Verify the caller request key makes retry idempotent before another epoch is allocated. */
    @Test
    void startJobReturnsExistingIntentForSameRequest() {
        JobControlIntent existing = persistedIntent();
        JobControlIntentDelivery delivery = persistedDelivery();
        when(intentRepository.findByRequestKey("request-start-1")).thenReturn(Optional.of(existing));
        when(deliveryRepository.findByJobControlIntentId(11L)).thenReturn(Optional.of(delivery));

        JobControlIntentService.JobControlIntentView result = service.startJob(
                "writer.orders",
                new JobControlIntentService.CreateJobControlIntentCommand(
                        "request-start-1", "alice", null));

        assertEquals(existing.getIntentKey(), result.intentKey());
        verify(writerJobBindingService, never()).allocateControlEpoch(any(), any());
    }

    /** Verify direct intent audit supports both present and blank keys. */
    @Test
    void findIntentReturnsPersistedAuditOnlyForValidKey() {
        JobControlIntent existing = persistedIntent();
        when(intentRepository.findByIntentKey(existing.getIntentKey())).thenReturn(Optional.of(existing));
        when(deliveryRepository.findByJobControlIntentId(11L)).thenReturn(Optional.of(persistedDelivery()));

        assertTrue(service.findIntent(existing.getIntentKey()).isPresent());
        assertTrue(service.findIntent(" ").isEmpty());
    }

    /** Assert the writer-side snapshot contract is complete. */
    @SuppressWarnings("unchecked")
    private void assertRequiredWriterProperties(
            Map<String, Object> payload,
            String intentKey,
            String epoch) {
        Map<String, Object> observation = (Map<String, Object>) payload.get("snapshotObservation");
        Map<String, String> properties = (Map<String, String>) observation.get("requiredWriterProperties");
        assertEquals("JOB_CONTROL", payload.get("intentKind"));
        assertEquals(intentKey, properties.get(SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY));
        assertEquals("writer.orders", properties.get(SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY));
        assertEquals(epoch, properties.get(SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY));
        assertNotNull(observation.get("confirmationTimeout"));
    }

    /** Build a registered streaming writer fixture. */
    private WriterJobBinding binding() {
        return WriterJobBinding.builder()
                .id(3L)
                .writerJobKey("writer.orders")
                .tableAssetKey("paimon.ods.orders")
                .allowedProcessingModes(List.of("STREAMING"))
                .currentWriterEpoch(0L)
                .build();
    }

    /** Build one persisted control intent for idempotency and audit tests. */
    private JobControlIntent persistedIntent() {
        return JobControlIntent.builder()
                .id(11L)
                .contractVersion(JobControlIntentContract.CONTRACT_VERSION)
                .requestKey("request-start-1")
                .intentKey("job-control:writer.orders:1")
                .writerJobBindingId(3L)
                .writerJobKey("writer.orders")
                .tableAssetKey("paimon.ods.orders")
                .operationType(JobControlOperations.START_JOB)
                .processingMode("STREAMING")
                .writerEpoch(1L)
                .previousWriterEpoch(0L)
                .snapshotResult(JobControlIntentContract.WAITING)
                .instructionPayloadJson(Map.of("intentKind", "JOB_CONTROL"))
                .build();
    }

    /** Build one persisted database delivery fixture. */
    private JobControlIntentDelivery persistedDelivery() {
        return JobControlIntentDelivery.builder()
                .id(21L)
                .jobControlIntentId(11L)
                .channel(SchedulingIntentDeliveryChannels.DATABASE_TABLE)
                .destination("job_control_intent")
                .status(SchedulingIntentDeliveryStatuses.PUBLISHED)
                .attemptCount(1)
                .build();
    }
}
