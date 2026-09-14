package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryStatusCount;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests independent job-control delivery counters, timers, and backlog gauges.
 */
class JobControlIntentDeliveryMetricsTest {

    /** Verify the first control dead letter can increase from a registered zero series. */
    @Test
    void constructorRegistersExhaustedSeries() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        new JobControlIntentDeliveryMetrics(registry, mock(JobControlIntentDeliveryRepository.class));

        assertEquals(0.0, registry.get("lakehouse.flow.job.control.intent.delivery.publisher.attempts")
                .tags("channel", "HTTP", "outcome", "exhausted")
                .counter()
                .count());
    }

    /** Verify one control transport attempt is not emitted under the data metric prefix. */
    @Test
    void recordAttemptPublishesJobControlEvidence() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JobControlIntentDeliveryRepository repository = mock(JobControlIntentDeliveryRepository.class);
        JobControlIntentDeliveryMetrics metrics = new JobControlIntentDeliveryMetrics(registry, repository);

        metrics.recordAttempt(publication(), "exhausted", Duration.ofMillis(12));

        assertEquals(1.0, registry.get("lakehouse.flow.job.control.intent.delivery.publisher.attempts")
                .tags("channel", "HTTP", "outcome", "exhausted")
                .counter()
                .count());
        assertEquals(1L, registry.get("lakehouse.flow.job.control.intent.delivery.publisher.attempt.duration")
                .tags("channel", "HTTP", "outcome", "exhausted")
                .timer()
                .count());
    }

    /** Verify grouped job-control rows refresh the dedicated backlog gauge. */
    @Test
    void refreshBacklogPublishesCurrentControlStateCounts() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JobControlIntentDeliveryRepository repository = mock(JobControlIntentDeliveryRepository.class);
        JobControlIntentDeliveryMetrics metrics = new JobControlIntentDeliveryMetrics(registry, repository);
        SchedulingIntentDeliveryStatusCount count = mock(SchedulingIntentDeliveryStatusCount.class);
        when(count.getChannel()).thenReturn(SchedulingIntentDeliveryChannels.HTTP);
        when(count.getStatus()).thenReturn(SchedulingIntentDeliveryStatuses.EXHAUSTED);
        when(count.getDeliveryCount()).thenReturn(2L);
        when(repository.countByChannelAndStatus())
                .thenReturn(List.of(count))
                .thenReturn(List.of());

        metrics.refreshBacklog();
        assertEquals(2.0, exhaustedBacklog(registry));

        metrics.refreshBacklog();
        assertEquals(0.0, exhaustedBacklog(registry));
    }

    /** Read the HTTP job-control dead-letter backlog gauge. */
    private double exhaustedBacklog(SimpleMeterRegistry registry) {
        return registry.get("lakehouse.flow.job.control.intent.delivery.records")
                .tags("channel", "HTTP", "status", "EXHAUSTED")
                .gauge()
                .value();
    }

    /** Build one claimed job-control publication fixture. */
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
