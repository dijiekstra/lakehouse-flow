package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.JobControlIntentDelivery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests read-only job-control delivery dead-letter queries.
 */
@ExtendWith(MockitoExtension.class)
class JobControlIntentDeliveryQueryServiceTest {

    @Mock
    private JobControlIntentDeliveryRepository deliveryRepository;

    @Mock
    private JobControlIntentRepository intentRepository;

    @InjectMocks
    private JobControlIntentDeliveryQueryService queryService;

    /** Verify the default query joins writer identity without inferring runtime failure. */
    @Test
    void findDeadLettersAcrossChannels() {
        JobControlIntentDelivery delivery = delivery();
        JobControlIntent intent = intent();
        when(deliveryRepository.findDeadLetters(
                SchedulingIntentDeliveryStatuses.EXHAUSTED,
                null,
                null,
                null,
                PageRequest.of(0, 100)))
                .thenReturn(List.of(delivery));
        when(intentRepository.findAllById(List.of(31L))).thenReturn(List.of(intent));

        List<JobControlIntentDeliveryQueryService.DeadLetterDelivery> result =
                queryService.findDeadLetters(null, null);

        assertEquals(1, result.size());
        assertEquals("job-control:writer.orders:4", result.get(0).intentKey());
        assertEquals("writer.orders", result.get(0).writerJobKey());
        assertEquals("connection refused", result.get(0).lastError());
    }

    /** Verify channel filters are normalized before querying. */
    @Test
    void findDeadLettersByChannel() {
        when(deliveryRepository.findDeadLetters(
                SchedulingIntentDeliveryStatuses.EXHAUSTED,
                SchedulingIntentDeliveryChannels.HTTP,
                "flow.orders",
                "lake.ods.orders",
                PageRequest.of(0, 20)))
                .thenReturn(List.of());
        when(intentRepository.findAllById(List.of())).thenReturn(List.of());

        assertEquals(0, queryService.findDeadLetters(
                " http ", " flow.orders ", "lake.ods.orders.dt=2026-09-14", 20).size());

        verify(deliveryRepository).findDeadLetters(
                SchedulingIntentDeliveryStatuses.EXHAUSTED,
                SchedulingIntentDeliveryChannels.HTTP,
                "flow.orders",
                "lake.ods.orders",
                PageRequest.of(0, 20));
    }

    /** Verify unsupported filters and unbounded requests fail early. */
    @Test
    void findDeadLettersRejectsInvalidFilters() {
        assertThrows(IllegalArgumentException.class, () -> queryService.findDeadLetters("FILE", 10));
        assertThrows(IllegalArgumentException.class, () -> queryService.findDeadLetters(null, 0));
        assertThrows(IllegalArgumentException.class, () -> queryService.findDeadLetters(null, 501));
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findDeadLetters(null, null, "invalid", 10));
    }

    /** Build one exhausted job-control transport fixture. */
    private JobControlIntentDelivery delivery() {
        return JobControlIntentDelivery.builder()
                .id(41L)
                .jobControlIntentId(31L)
                .channel(SchedulingIntentDeliveryChannels.HTTP)
                .destination("https://platform.example/job-control")
                .status(SchedulingIntentDeliveryStatuses.EXHAUSTED)
                .attemptCount(8)
                .lastError("connection refused")
                .deadLetteredAt(LocalDateTime.of(2026, 9, 14, 10, 0))
                .build();
    }

    /** Build the immutable writer lifecycle instruction joined into the operations view. */
    private JobControlIntent intent() {
        return JobControlIntent.builder()
                .id(31L)
                .intentKey("job-control:writer.orders:4")
                .writerJobKey("writer.orders")
                .tableAssetKey("lake.ods.orders")
                .operationType("RESTART_JOB")
                .writerEpoch(4L)
                .build();
    }
}
