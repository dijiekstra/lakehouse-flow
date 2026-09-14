package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryStatusCount;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer instrumentation for job-control intent transport attempts and backlog.
 *
 * <p>Control metrics are deliberately separate from data-processing intent metrics so
 * START/RESTART transport failures cannot be mistaken for data scheduling failures.
 */
@Component
public class JobControlIntentDeliveryMetrics {

    private static final String METRIC_PREFIX = "lakehouse.flow.job.control.intent.delivery";
    private static final List<String> CHANNELS = List.of(
            SchedulingIntentDeliveryChannels.DATABASE_TABLE,
            SchedulingIntentDeliveryChannels.HTTP,
            SchedulingIntentDeliveryChannels.MQ);
    private static final List<String> STATUSES = List.of(
            SchedulingIntentDeliveryStatuses.PENDING,
            SchedulingIntentDeliveryStatuses.PUBLISHING,
            SchedulingIntentDeliveryStatuses.PUBLISHED,
            SchedulingIntentDeliveryStatuses.RETRY_WAIT,
            SchedulingIntentDeliveryStatuses.EXHAUSTED);
    private static final List<String> ATTEMPT_OUTCOMES = List.of(
            "published",
            "retry_scheduled",
            "exhausted",
            "stale",
            "stale_acknowledgement");

    private final MeterRegistry meterRegistry;
    private final JobControlIntentDeliveryRepository deliveryRepository;
    private final Map<BacklogKey, AtomicLong> backlog = new ConcurrentHashMap<>();

    /**
     * Create job-control delivery metrics and register bounded backlog gauges.
     *
     * @param meterRegistry application meter registry
     * @param deliveryRepository job-control delivery repository
     */
    public JobControlIntentDeliveryMetrics(
            MeterRegistry meterRegistry,
            JobControlIntentDeliveryRepository deliveryRepository) {
        this.meterRegistry = meterRegistry;
        this.deliveryRepository = deliveryRepository;
        CHANNELS.forEach(channel -> {
            STATUSES.forEach(status -> registerBacklogGauge(channel, status));
            ATTEMPT_OUTCOMES.forEach(outcome -> registerAttemptMeters(channel, outcome));
        });
    }

    /**
     * Record one claimed job-control transport attempt and its fenced result.
     *
     * @param publication claimed immutable control publication
     * @param outcome transport outcome such as published, retry_scheduled, exhausted, or stale
     * @param duration publisher call duration
     */
    public void recordAttempt(
            SchedulingIntentPublication publication,
            String outcome,
            Duration duration) {
        Counter.builder(METRIC_PREFIX + ".publisher.attempts")
                .tags("channel", publication.channel(), "outcome", outcome)
                .register(meterRegistry)
                .increment();
        Timer.builder(METRIC_PREFIX + ".publisher.attempt.duration")
                .tags("channel", publication.channel(), "outcome", outcome)
                .register(meterRegistry)
                .record(duration);
    }

    /** Refresh current job-control delivery backlog gauges from one grouped query. */
    @Scheduled(fixedDelayString = "${lakehouse-flow.job-control-intent-delivery.metrics.fixed-delay-ms:10000}")
    public void refreshBacklog() {
        backlog.values().forEach(value -> value.set(0L));
        for (SchedulingIntentDeliveryStatusCount count : deliveryRepository.countByChannelAndStatus()) {
            backlog.computeIfAbsent(
                            new BacklogKey(count.getChannel(), count.getStatus()),
                            key -> registerBacklogGauge(key.channel(), key.status()))
                    .set(count.getDeliveryCount());
        }
    }

    /** Register one job-control channel and status backlog gauge. */
    private AtomicLong registerBacklogGauge(String channel, String status) {
        BacklogKey key = new BacklogKey(channel, status);
        AtomicLong value = new AtomicLong();
        AtomicLong existing = backlog.putIfAbsent(key, value);
        if (existing != null) {
            return existing;
        }
        Gauge.builder(METRIC_PREFIX + ".records", value, AtomicLong::get)
                .tags("channel", channel, "status", status)
                .register(meterRegistry);
        return value;
    }

    /** Register a bounded control attempt counter and timer before the first failure. */
    private void registerAttemptMeters(String channel, String outcome) {
        Counter.builder(METRIC_PREFIX + ".publisher.attempts")
                .tags("channel", channel, "outcome", outcome)
                .register(meterRegistry);
        Timer.builder(METRIC_PREFIX + ".publisher.attempt.duration")
                .tags("channel", channel, "outcome", outcome)
                .register(meterRegistry);
    }

    /** Stable gauge key for one job-control transport channel and state. */
    private record BacklogKey(String channel, String status) {
    }
}
