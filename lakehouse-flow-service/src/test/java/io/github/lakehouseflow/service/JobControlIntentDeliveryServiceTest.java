package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.JobControlIntentDelivery;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests reliable claim, fencing, retry, and exhaustion for control intents.
 */
@ExtendWith(MockitoExtension.class)
class JobControlIntentDeliveryServiceTest {

    @Mock
    private JobControlIntentDeliveryRepository deliveryRepository;

    @Mock
    private JobControlIntentRepository intentRepository;

    @InjectMocks
    private JobControlIntentDeliveryService service;

    /** Verify a due control delivery is fenced and projected to the shared publisher contract. */
    @Test
    void claimDueDeliveriesReturnsImmutablePublication() {
        JobControlIntentDelivery delivery = delivery(SchedulingIntentDeliveryStatuses.PENDING, null);
        when(deliveryRepository.findClaimableForUpdate(
                org.mockito.ArgumentMatchers.<Collection<String>>any(),
                eq(SchedulingIntentDeliveryStatuses.PUBLISHING),
                any(LocalDateTime.class),
                any(Pageable.class))).thenReturn(List.of(delivery));
        when(intentRepository.findById(11L)).thenReturn(Optional.of(intent()));

        List<SchedulingIntentPublication> publications = service.claimDueDeliveries(
                "scheduler-a", 10, Duration.ofSeconds(30));

        assertEquals(1, publications.size());
        assertEquals("job-control:writer.orders:1", publications.get(0).intentKey());
        assertNotNull(publications.get(0).claimToken());
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHING, delivery.getStatus());
        assertEquals(1, delivery.getAttemptCount());
    }

    /** Verify an expired delivery is dead-lettered before any network publication. */
    @Test
    void claimDueDeliveriesExhaustsExpiredAdmission() {
        JobControlIntentDelivery delivery = delivery(
                SchedulingIntentDeliveryStatuses.PENDING,
                LocalDateTime.now().minusSeconds(1));
        when(deliveryRepository.findClaimableForUpdate(
                org.mockito.ArgumentMatchers.<Collection<String>>any(),
                eq(SchedulingIntentDeliveryStatuses.PUBLISHING),
                any(LocalDateTime.class),
                any(Pageable.class))).thenReturn(List.of(delivery));

        assertTrue(service.claimDueDeliveries("scheduler-a", null, Duration.ofSeconds(30)).isEmpty());
        assertEquals(SchedulingIntentDeliveryStatuses.EXHAUSTED, delivery.getStatus());
        assertNotNull(delivery.getDeadLetteredAt());
    }

    /** Verify publication acknowledgement requires the current claim token. */
    @Test
    void recordPublishedRejectsStaleAndAcceptsCurrentClaim() {
        JobControlIntentDelivery stale = delivery(SchedulingIntentDeliveryStatuses.PUBLISHING, null);
        stale.setClaimToken("new-token");
        JobControlIntentDelivery current = delivery(SchedulingIntentDeliveryStatuses.PUBLISHING, null);
        current.setClaimToken("current-token");
        when(deliveryRepository.findByIdForUpdate(21L))
                .thenReturn(Optional.of(stale))
                .thenReturn(Optional.of(current));

        assertFalse(service.recordPublished(21L, "old-token"));
        assertTrue(service.recordPublished(21L, "current-token"));
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHED, current.getStatus());
    }

    /** Verify a repeated control acknowledgement remains idempotent without an audit rewrite. */
    @Test
    void recordPublishedAcceptsAlreadyPublishedDeliveryWithoutRewrite() {
        JobControlIntentDelivery published = delivery(SchedulingIntentDeliveryStatuses.PUBLISHED, null);
        published.setPublishedAt(LocalDateTime.now().minusMinutes(1));
        when(deliveryRepository.findByIdForUpdate(21L)).thenReturn(Optional.of(published));

        assertTrue(service.recordPublished(21L, "expired-claim"));

        verify(deliveryRepository, never()).save(any(JobControlIntentDelivery.class));
    }

    /** Verify transport failure first retries and then exhausts at the attempt limit. */
    @Test
    void recordFailureSchedulesRetryAndExhaustion() {
        JobControlIntentDelivery retry = delivery(SchedulingIntentDeliveryStatuses.PUBLISHING, null);
        retry.setAttemptCount(1);
        retry.setClaimToken("retry-token");
        JobControlIntentDelivery exhausted = delivery(SchedulingIntentDeliveryStatuses.PUBLISHING, null);
        exhausted.setAttemptCount(3);
        exhausted.setClaimToken("final-token");
        when(deliveryRepository.findByIdForUpdate(21L))
                .thenReturn(Optional.of(retry))
                .thenReturn(Optional.of(exhausted));

        SchedulingIntentDeliveryFailureResult retryResult = service.recordFailure(
                21L, "retry-token", "timeout", 3, Duration.ofSeconds(1), Duration.ofMinutes(1));
        SchedulingIntentDeliveryFailureResult exhaustedResult = service.recordFailure(
                21L, "final-token", "timeout", 3, Duration.ofSeconds(1), Duration.ofMinutes(1));

        assertEquals(SchedulingIntentDeliveryFailureResult.RETRY_SCHEDULED, retryResult);
        assertEquals(SchedulingIntentDeliveryStatuses.RETRY_WAIT, retry.getStatus());
        assertEquals(SchedulingIntentDeliveryFailureResult.EXHAUSTED, exhaustedResult);
        assertEquals(SchedulingIntentDeliveryStatuses.EXHAUSTED, exhausted.getStatus());
    }

    /** Build one immutable job-control intent fixture. */
    private JobControlIntent intent() {
        return JobControlIntent.builder()
                .id(11L)
                .contractVersion("1.0")
                .intentKey("job-control:writer.orders:1")
                .instructionPayloadJson(Map.of("intentKind", "JOB_CONTROL"))
                .build();
    }

    /** Build one transport fixture with a future default deadline. */
    private JobControlIntentDelivery delivery(String status, LocalDateTime deliverBefore) {
        return JobControlIntentDelivery.builder()
                .id(21L)
                .jobControlIntentId(11L)
                .channel(SchedulingIntentDeliveryChannels.HTTP)
                .destination("https://platform.example/jobs")
                .status(status)
                .attemptCount(0)
                .deliverBefore(deliverBefore == null ? LocalDateTime.now().plusHours(1) : deliverBefore)
                .createdAt(LocalDateTime.now().minusMinutes(1))
                .build();
    }
}
