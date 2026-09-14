package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests fenced delivery claims, acknowledgement, retry, and dead-letter evidence.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingIntentDeliveryServiceTest {

    @Mock
    private SchedulingIntentDeliveryRepository schedulingIntentDeliveryRepository;

    @Mock
    private SchedulingIntentRepository schedulingIntentRepository;

    @InjectMocks
    private SchedulingIntentDeliveryService schedulingIntentDeliveryService;

    /** Verify a due delivery receives an incremented attempt and fencing token. */
    @Test
    void claimDueDeliveriesClaimsPendingTransport() {
        SchedulingIntentDelivery delivery = pendingDelivery(LocalDateTime.now().plusHours(1));
        when(schedulingIntentDeliveryRepository.findClaimableForUpdate(
                eq(Set.of(SchedulingIntentDeliveryStatuses.PENDING,
                        SchedulingIntentDeliveryStatuses.RETRY_WAIT)),
                eq(SchedulingIntentDeliveryStatuses.PUBLISHING),
                any(LocalDateTime.class),
                eq(PageRequest.of(0, 10))))
                .thenReturn(List.of(delivery));
        when(schedulingIntentRepository.findById(101L)).thenReturn(Optional.of(intent()));
        when(schedulingIntentDeliveryRepository.save(delivery)).thenReturn(delivery);

        List<SchedulingIntentPublication> result = schedulingIntentDeliveryService.claimDueDeliveries(
                "scheduler-a",
                10,
                Duration.ofSeconds(30));

        assertEquals(1, result.size());
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHING, delivery.getStatus());
        assertEquals(1, delivery.getAttemptCount());
        assertEquals("scheduler-a", delivery.getClaimOwner());
        assertNotNull(delivery.getClaimToken());
        assertNotNull(delivery.getClaimExpiresAt());
        assertEquals("task-instance:22", result.get(0).intentKey());
        assertEquals(delivery.getClaimToken(), result.get(0).claimToken());
    }

    /** Verify an abandoned in-flight attempt can be fenced and reclaimed. */
    @Test
    void claimDueDeliveriesReclaimsExpiredAttempt() {
        SchedulingIntentDelivery delivery = pendingDelivery(LocalDateTime.now().plusHours(1));
        delivery.setStatus(SchedulingIntentDeliveryStatuses.PUBLISHING);
        delivery.setAttemptCount(2);
        delivery.setClaimOwner("old-owner");
        delivery.setClaimToken("old-token");
        delivery.setClaimExpiresAt(LocalDateTime.now().minusSeconds(1));
        when(schedulingIntentDeliveryRepository.findClaimableForUpdate(
                any(), eq(SchedulingIntentDeliveryStatuses.PUBLISHING), any(), any()))
                .thenReturn(List.of(delivery));
        when(schedulingIntentRepository.findById(101L)).thenReturn(Optional.of(intent()));

        List<SchedulingIntentPublication> result = schedulingIntentDeliveryService.claimDueDeliveries(
                "scheduler-b", null, Duration.ofSeconds(20));

        assertEquals(1, result.size());
        assertEquals(3, delivery.getAttemptCount());
        assertEquals("scheduler-b", delivery.getClaimOwner());
        assertFalse("old-token".equals(delivery.getClaimToken()));
    }

    /** Verify an intent is dead-lettered instead of published after its admission deadline. */
    @Test
    void claimDueDeliveriesDeadLettersExpiredPublication() {
        SchedulingIntentDelivery delivery = pendingDelivery(LocalDateTime.now().minusSeconds(1));
        when(schedulingIntentDeliveryRepository.findClaimableForUpdate(any(), any(), any(), any()))
                .thenReturn(List.of(delivery));

        List<SchedulingIntentPublication> result = schedulingIntentDeliveryService.claimDueDeliveries(
                "scheduler-a", 10, Duration.ofSeconds(30));

        assertTrue(result.isEmpty());
        assertEquals(SchedulingIntentDeliveryStatuses.EXHAUSTED, delivery.getStatus());
        assertNotNull(delivery.getDeadLetteredAt());
        assertTrue(delivery.getLastError().contains("expired"));
        verify(schedulingIntentRepository, never()).findById(any());
    }

    /** Verify transport acknowledgement completes only the current fenced claim. */
    @Test
    void recordPublishedCompletesCurrentClaim() {
        SchedulingIntentDelivery delivery = claimedDelivery("claim-1", 1);
        when(schedulingIntentDeliveryRepository.findByIdForUpdate(201L)).thenReturn(Optional.of(delivery));

        boolean recorded = schedulingIntentDeliveryService.recordPublished(201L, "claim-1");

        assertTrue(recorded);
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHED, delivery.getStatus());
        assertNotNull(delivery.getPublishedAt());
        assertNull(delivery.getClaimToken());
        assertNull(delivery.getLastError());
        verify(schedulingIntentDeliveryRepository).save(delivery);
    }

    /** Verify a stale acknowledgement cannot overwrite a newer claim. */
    @Test
    void recordPublishedIgnoresStaleClaim() {
        SchedulingIntentDelivery delivery = claimedDelivery("new-token", 2);
        when(schedulingIntentDeliveryRepository.findByIdForUpdate(201L)).thenReturn(Optional.of(delivery));

        assertFalse(schedulingIntentDeliveryService.recordPublished(201L, "old-token"));

        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHING, delivery.getStatus());
        verify(schedulingIntentDeliveryRepository, never()).save(any());
    }

    /** Verify a repeated acknowledgement is idempotent and does not rewrite audit timestamps. */
    @Test
    void recordPublishedAcceptsAlreadyPublishedDeliveryWithoutRewrite() {
        SchedulingIntentDelivery delivery = claimedDelivery(null, 1);
        delivery.setStatus(SchedulingIntentDeliveryStatuses.PUBLISHED);
        delivery.setPublishedAt(LocalDateTime.now().minusMinutes(1));
        when(schedulingIntentDeliveryRepository.findByIdForUpdate(201L)).thenReturn(Optional.of(delivery));

        assertTrue(schedulingIntentDeliveryService.recordPublished(201L, "old-claim"));

        verify(schedulingIntentDeliveryRepository, never()).save(any());
    }

    /** Verify a failed attempt enters retry wait with bounded exponential delay. */
    @Test
    void recordFailureSchedulesRetry() {
        SchedulingIntentDelivery delivery = claimedDelivery("claim-1", 2);
        delivery.setDeliverBefore(LocalDateTime.now().plusHours(1));
        when(schedulingIntentDeliveryRepository.findByIdForUpdate(201L)).thenReturn(Optional.of(delivery));
        LocalDateTime before = LocalDateTime.now();

        SchedulingIntentDeliveryFailureResult result = schedulingIntentDeliveryService.recordFailure(
                201L,
                "claim-1",
                "broker unavailable",
                5,
                Duration.ofSeconds(2),
                Duration.ofMinutes(1));

        assertEquals(SchedulingIntentDeliveryFailureResult.RETRY_SCHEDULED, result);
        assertEquals(SchedulingIntentDeliveryStatuses.RETRY_WAIT, delivery.getStatus());
        assertEquals("broker unavailable", delivery.getLastError());
        assertNull(delivery.getClaimToken());
        assertTrue(!delivery.getNextAttemptAt().isBefore(before.plusSeconds(4)));
        assertNull(delivery.getDeadLetteredAt());
    }

    /** Verify reaching the retry limit records terminal transport dead-letter evidence. */
    @Test
    void recordFailureDeadLettersExhaustedTransport() {
        SchedulingIntentDelivery delivery = claimedDelivery("claim-1", 3);
        delivery.setDeliverBefore(LocalDateTime.now().plusHours(1));
        when(schedulingIntentDeliveryRepository.findByIdForUpdate(201L)).thenReturn(Optional.of(delivery));

        assertEquals(SchedulingIntentDeliveryFailureResult.EXHAUSTED,
                schedulingIntentDeliveryService.recordFailure(
                201L,
                "claim-1",
                "endpoint unavailable",
                3,
                Duration.ofSeconds(1),
                Duration.ofMinutes(1)));

        assertEquals(SchedulingIntentDeliveryStatuses.EXHAUSTED, delivery.getStatus());
        assertNotNull(delivery.getDeadLetteredAt());
        assertNull(delivery.getNextAttemptAt());
    }

    /** Verify invalid retry policy is rejected before mutating delivery evidence. */
    @Test
    void recordFailureRejectsInvalidRetryPolicy() {
        assertThrows(IllegalArgumentException.class, () -> schedulingIntentDeliveryService.recordFailure(
                201L,
                "claim-1",
                "failure",
                0,
                Duration.ofSeconds(1),
                Duration.ofMinutes(1)));
        assertThrows(IllegalArgumentException.class, () -> schedulingIntentDeliveryService.recordFailure(
                201L,
                "claim-1",
                "failure",
                2,
                Duration.ofMinutes(2),
                Duration.ofMinutes(1)));
    }

    /** Build one pending HTTP delivery fixture. */
    private SchedulingIntentDelivery pendingDelivery(LocalDateTime deliverBefore) {
        return SchedulingIntentDelivery.builder()
                .id(201L)
                .schedulingIntentId(101L)
                .channel(SchedulingIntentDeliveryChannels.HTTP)
                .destination("https://downstream.example/intents")
                .status(SchedulingIntentDeliveryStatuses.PENDING)
                .attemptCount(0)
                .deliverBefore(deliverBefore)
                .createdAt(LocalDateTime.now().minusMinutes(1))
                .build();
    }

    /** Build one currently claimed delivery fixture. */
    private SchedulingIntentDelivery claimedDelivery(String claimToken, int attemptCount) {
        return SchedulingIntentDelivery.builder()
                .id(201L)
                .schedulingIntentId(101L)
                .channel(SchedulingIntentDeliveryChannels.HTTP)
                .destination("https://downstream.example/intents")
                .status(SchedulingIntentDeliveryStatuses.PUBLISHING)
                .attemptCount(attemptCount)
                .claimOwner("scheduler-a")
                .claimToken(claimToken)
                .claimExpiresAt(LocalDateTime.now().plusSeconds(30))
                .build();
    }

    /** Build the immutable intent payload loaded for a claim. */
    private SchedulingIntent intent() {
        return SchedulingIntent.builder()
                .id(101L)
                .contractVersion("1.2")
                .intentKey("task-instance:22")
                .instructionPayloadJson(Map.of("intentKey", "task-instance:22"))
                .build();
    }
}
