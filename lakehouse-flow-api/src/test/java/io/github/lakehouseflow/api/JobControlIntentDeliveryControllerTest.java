package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.JobControlIntentDeadLetterResponse;
import io.github.lakehouseflow.service.JobControlIntentDeliveryQueryService;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the read-only job-control delivery dead-letter API mapping.
 */
class JobControlIntentDeliveryControllerTest {

    /** Verify transport dead letters expose writer identity without runtime status. */
    @Test
    void findDeadLettersMapsOperationsEvidence() {
        JobControlIntentDeliveryQueryService queryService = mock(JobControlIntentDeliveryQueryService.class);
        JobControlIntentDeliveryController controller = new JobControlIntentDeliveryController(queryService);
        LocalDateTime deadLetteredAt = LocalDateTime.of(2026, 9, 14, 10, 0);
        when(queryService.findDeadLetters("HTTP", 25)).thenReturn(List.of(
                new JobControlIntentDeliveryQueryService.DeadLetterDelivery(
                        41L,
                        31L,
                        "job-control:writer.orders:4",
                        "writer.orders",
                        "lake.ods.orders",
                        "RESTART_JOB",
                        4L,
                        "HTTP",
                        "https://platform.example/job-control",
                        8,
                        "connection refused",
                        deadLetteredAt.minusSeconds(1),
                        deadLetteredAt.minusMinutes(1),
                        deadLetteredAt)));

        List<JobControlIntentDeadLetterResponse> result = controller.findDeadLetters("HTTP", 25);

        assertEquals(1, result.size());
        assertEquals("writer.orders", result.get(0).writerJobKey());
        assertEquals("connection refused", result.get(0).lastError());
        assertEquals(deadLetteredAt, result.get(0).deadLetteredAt());
    }
}
