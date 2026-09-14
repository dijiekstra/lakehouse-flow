package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.CreateJobControlIntentRequest;
import io.github.lakehouseflow.api.dto.CreateWriterJobBindingRequest;
import io.github.lakehouseflow.api.dto.JobControlIntentResponse;
import io.github.lakehouseflow.api.dto.WriterJobBindingResponse;
import io.github.lakehouseflow.common.JobControlIntentContract;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.model.WriterJobBinding;
import io.github.lakehouseflow.service.JobControlIntentService;
import io.github.lakehouseflow.service.WriterJobBindingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests platform writer ownership and lifecycle API mappings.
 */
@ExtendWith(MockitoExtension.class)
class WriterJobControllerTest {

    @Mock
    private WriterJobBindingService bindingService;

    @Mock
    private JobControlIntentService intentService;

    /** Verify binding registration and lookup expose writer fencing coordinates. */
    @Test
    void createAndReadBindingMapsServiceModel() {
        WriterJobController controller = new WriterJobController(bindingService, intentService);
        WriterJobBinding binding = binding();
        when(bindingService.createBinding(any())).thenReturn(binding);
        when(bindingService.findBinding("writer.orders")).thenReturn(Optional.of(binding));

        WriterJobBindingResponse created = controller.createBinding(new CreateWriterJobBindingRequest(
                "writer.orders", "paimon.ods.orders", List.of("STREAMING")));

        assertEquals("paimon.ods.orders", created.tableAssetKey());
        assertEquals(7L, created.currentWriterEpoch());
        assertEquals(HttpStatus.OK, controller.getBinding("writer.orders").getStatusCode());
    }

    /** Verify start and restart endpoints remain independent from task APIs. */
    @Test
    void startAndRestartMapIndependentControlIntent() {
        WriterJobController controller = new WriterJobController(bindingService, intentService);
        JobControlIntentService.JobControlIntentView view = intentView();
        when(intentService.startJob(any(), any())).thenReturn(view);
        when(intentService.restartJob(any(), any())).thenReturn(view);
        CreateJobControlIntentRequest request = new CreateJobControlIntentRequest(
                "request-7", "alice", "platform recovery");

        JobControlIntentResponse started = controller.startJob("writer.orders", request);
        JobControlIntentResponse restarted = controller.restartJob("writer.orders", request);

        assertEquals("job-control:writer.orders:7", started.intentKey());
        assertEquals(JobControlIntentContract.WAITING, restarted.snapshotResult());
    }

    /** Verify job-control audit returns both present and absent HTTP semantics. */
    @Test
    void getControlIntentReturnsPresentOrNotFound() {
        WriterJobController controller = new WriterJobController(bindingService, intentService);
        when(intentService.findIntent("job-control:writer.orders:7"))
                .thenReturn(Optional.of(intentView()));
        when(intentService.findIntent("missing")).thenReturn(Optional.empty());

        assertEquals(HttpStatus.OK,
                controller.getControlIntent("job-control:writer.orders:7").getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND, controller.getControlIntent("missing").getStatusCode());
    }

    /** Build one writer binding API fixture. */
    private WriterJobBinding binding() {
        return WriterJobBinding.builder()
                .id(3L)
                .writerJobKey("writer.orders")
                .tableAssetKey("paimon.ods.orders")
                .allowedProcessingModes(List.of("STREAMING"))
                .currentWriterEpoch(7L)
                .activeProcessingMode("STREAMING")
                .currentControlIntentKey("job-control:writer.orders:7")
                .build();
    }

    /** Build one independent control intent API fixture. */
    private JobControlIntentService.JobControlIntentView intentView() {
        LocalDateTime now = LocalDateTime.of(2026, 9, 14, 10, 0);
        return new JobControlIntentService.JobControlIntentView(
                11L, "1.0", "request-7", "job-control:writer.orders:7",
                "writer.orders", "paimon.ods.orders", "RESTART_JOB", "STREAMING",
                7L, 6L, "811", now.plusMinutes(10), now.plusMinutes(10), "alice",
                "platform recovery", JobControlIntentContract.WAITING, null, "waiting", null,
                null, null, now, Map.of("intentKind", "JOB_CONTROL"),
                SchedulingIntentDeliveryChannels.DATABASE_TABLE, "job_control_intent",
                SchedulingIntentDeliveryStatuses.PUBLISHED, 1, null, now, null, null, now, now);
    }
}
