package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.service.JobControlIntentDeliveryService;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests scheduled publication of independent job-control intents.
 */
@ExtendWith(MockitoExtension.class)
class JobControlIntentDeliveryScannerTest {

    @Mock
    private JobControlIntentDeliveryService deliveryService;

    @Mock
    private SchedulingIntentPublisher publisher;

    @Mock
    private JobControlIntentDeliveryMetrics deliveryMetrics;

    private JobControlIntentDeliveryScanner scanner;

    /** Configure deterministic control-delivery settings for each test. */
    @BeforeEach
    void setUp() {
        scanner = new JobControlIntentDeliveryScanner(deliveryService, List.of(publisher), deliveryMetrics);
        ReflectionTestUtils.setField(scanner, "configuredOwner", "control-scheduler-test");
        ReflectionTestUtils.setField(scanner, "batchSize", 20);
        ReflectionTestUtils.setField(scanner, "claimLease", Duration.ofSeconds(30));
        ReflectionTestUtils.setField(scanner, "maxAttempts", 5);
        ReflectionTestUtils.setField(scanner, "initialBackoff", Duration.ofSeconds(2));
        ReflectionTestUtils.setField(scanner, "maximumBackoff", Duration.ofMinutes(2));
    }

    /** Verify transport acknowledgement is persisted without creating execution state. */
    @Test
    void publishDueDeliveriesRecordsTransportAcknowledgement() {
        SchedulingIntentPublication publication = publication();
        when(publisher.channel()).thenReturn(SchedulingIntentDeliveryChannels.HTTP);
        when(deliveryService.claimDueDeliveries(
                "control-scheduler-test", 20, Duration.ofSeconds(30)))
                .thenReturn(List.of(publication));
        when(deliveryService.recordPublished(301L, "control-claim")).thenReturn(true);

        scanner.publishDueDeliveries();

        verify(publisher).publish(publication);
        verify(deliveryService).recordPublished(301L, "control-claim");
        verify(deliveryMetrics).recordAttempt(eq(publication), eq("published"), any(Duration.class));
    }

    /** Verify a failed control transport uses the same fenced retry policy as data intents. */
    @Test
    void publishDueDeliveriesRecordsTransportFailure() {
        SchedulingIntentPublication publication = publication();
        when(publisher.channel()).thenReturn(SchedulingIntentDeliveryChannels.HTTP);
        when(deliveryService.claimDueDeliveries(
                "control-scheduler-test", 20, Duration.ofSeconds(30)))
                .thenReturn(List.of(publication));
        doThrow(new IllegalStateException("platform endpoint unavailable"))
                .when(publisher).publish(publication);
        when(deliveryService.recordFailure(
                301L,
                "control-claim",
                "platform endpoint unavailable",
                5,
                Duration.ofSeconds(2),
                Duration.ofMinutes(2)))
                .thenReturn(SchedulingIntentDeliveryFailureResult.RETRY_SCHEDULED);

        scanner.publishDueDeliveries();

        verify(deliveryService).recordFailure(
                301L,
                "control-claim",
                "platform endpoint unavailable",
                5,
                Duration.ofSeconds(2),
                Duration.ofMinutes(2));
        verify(deliveryMetrics).recordAttempt(
                eq(publication), eq("retry_scheduled"), any(Duration.class));
    }

    /** Build one claimed HTTP control publication fixture. */
    private SchedulingIntentPublication publication() {
        return new SchedulingIntentPublication(
                301L,
                "control-claim",
                SchedulingIntentDeliveryChannels.HTTP,
                "https://platform.example/job-control",
                401L,
                "job-control:writer.ods.orders:2",
                "1.0",
                Map.of("intentKind", "JOB_CONTROL"));
    }
}
