package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
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
 * Tests delivery counters, timers, and grouped backlog gauges.
 */
class SchedulingIntentDeliveryMetricsTest {

    /** Verify one transport attempt increments its tagged counter and timer. */
    @Test
    void recordAttemptPublishesMicrometerEvidence() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SchedulingIntentDeliveryRepository repository = mock(SchedulingIntentDeliveryRepository.class);
        SchedulingIntentDeliveryMetrics metrics = new SchedulingIntentDeliveryMetrics(registry, repository);
        SchedulingIntentPublication publication = publication();

        metrics.recordAttempt(publication, "published", Duration.ofMillis(12));

        assertEquals(1.0, registry.get("lakehouse.flow.scheduling.intent.delivery.publisher.attempts")
                .tags("channel", "HTTP", "outcome", "published")
                .counter()
                .count());
        assertEquals(1L, registry.get("lakehouse.flow.scheduling.intent.delivery.publisher.attempt.duration")
                .tags("channel", "HTTP", "outcome", "published")
                .timer()
                .count());
    }

    /** Verify one grouped query refreshes current backlog gauges and clears stale values. */
    @Test
    void refreshBacklogPublishesCurrentStateCounts() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SchedulingIntentDeliveryRepository repository = mock(SchedulingIntentDeliveryRepository.class);
        SchedulingIntentDeliveryMetrics metrics = new SchedulingIntentDeliveryMetrics(registry, repository);
        SchedulingIntentDeliveryStatusCount count = mock(SchedulingIntentDeliveryStatusCount.class);
        when(count.getChannel()).thenReturn(SchedulingIntentDeliveryChannels.MQ);
        when(count.getStatus()).thenReturn(SchedulingIntentDeliveryStatuses.EXHAUSTED);
        when(count.getDeliveryCount()).thenReturn(3L);
        when(repository.countByChannelAndStatus())
                .thenReturn(List.of(count))
                .thenReturn(List.of());

        metrics.refreshBacklog();
        assertEquals(3.0, backlog(registry));

        metrics.refreshBacklog();
        assertEquals(0.0, backlog(registry));
    }

    /** Read the MQ dead-letter backlog gauge. */
    private double backlog(SimpleMeterRegistry registry) {
        return registry.get("lakehouse.flow.scheduling.intent.delivery.records")
                .tags("channel", "MQ", "status", "EXHAUSTED")
                .gauge()
                .value();
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
