package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.service.SchedulingIntentDeliveryService;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublisher;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

/**
 * Tests scanner orchestration around network-free claim and fenced completion calls.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingIntentDeliveryScannerTest {

    @Mock
    private SchedulingIntentDeliveryService deliveryService;

    @Mock
    private SchedulingIntentPublisher publisher;

    @Mock
    private SchedulingIntentDeliveryMetrics deliveryMetrics;

    private SchedulingIntentDeliveryScanner scanner;

    /** Configure deterministic retry settings for each scanner test. */
    @BeforeEach
    void setUp() {
        scanner = new SchedulingIntentDeliveryScanner(deliveryService, List.of(publisher), deliveryMetrics);
        ReflectionTestUtils.setField(scanner, "configuredOwner", "scheduler-test");
        ReflectionTestUtils.setField(scanner, "batchSize", 25);
        ReflectionTestUtils.setField(scanner, "claimLease", Duration.ofSeconds(20));
        ReflectionTestUtils.setField(scanner, "maxAttempts", 4);
        ReflectionTestUtils.setField(scanner, "initialBackoff", Duration.ofSeconds(2));
        ReflectionTestUtils.setField(scanner, "maximumBackoff", Duration.ofMinutes(1));
    }

    /** Verify a successful transport call records only the fenced acknowledgement. */
    @Test
    void publishDueDeliveriesRecordsTransportAcknowledgement() {
        SchedulingIntentPublication publication = publication();
        when(publisher.channel()).thenReturn(SchedulingIntentDeliveryChannels.HTTP);
        when(deliveryService.claimDueDeliveries(
                "scheduler-test", 25, Duration.ofSeconds(20)))
                .thenReturn(List.of(publication));
        when(deliveryService.recordPublished(201L, "claim-token")).thenReturn(true);

        scanner.publishDueDeliveries();

        verify(publisher).publish(publication);
        verify(deliveryService).recordPublished(201L, "claim-token");
        verify(deliveryMetrics).recordAttempt(
                org.mockito.ArgumentMatchers.eq(publication),
                org.mockito.ArgumentMatchers.eq("published"),
                any(Duration.class));
    }

    /** Verify a publisher exception is converted to retryable transport evidence. */
    @Test
    void publishDueDeliveriesRecordsTransportFailure() {
        SchedulingIntentPublication publication = publication();
        when(publisher.channel()).thenReturn(SchedulingIntentDeliveryChannels.HTTP);
        when(deliveryService.claimDueDeliveries(
                "scheduler-test", 25, Duration.ofSeconds(20)))
                .thenReturn(List.of(publication));
        doThrow(new IllegalStateException("endpoint unavailable")).when(publisher).publish(publication);
        when(deliveryService.recordFailure(
                201L,
                "claim-token",
                "endpoint unavailable",
                4,
                Duration.ofSeconds(2),
                Duration.ofMinutes(1)))
                .thenReturn(SchedulingIntentDeliveryFailureResult.RETRY_SCHEDULED);

        scanner.publishDueDeliveries();

        verify(deliveryService).recordFailure(
                201L,
                "claim-token",
                "endpoint unavailable",
                4,
                Duration.ofSeconds(2),
                Duration.ofMinutes(1));
        verify(deliveryMetrics).recordAttempt(
                org.mockito.ArgumentMatchers.eq(publication),
                org.mockito.ArgumentMatchers.eq("retry_scheduled"),
                any(Duration.class));
    }

    /** Build one claimed HTTP publication fixture. */
    private SchedulingIntentPublication publication() {
        return new SchedulingIntentPublication(
                201L,
                "claim-token",
                SchedulingIntentDeliveryChannels.HTTP,
                "https://downstream.example/intents",
                101L,
                "task-instance:22",
                "1.2",
                Map.of("intentKey", "task-instance:22"));
    }
}
