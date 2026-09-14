package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.service.JobControlIntentDeliveryService;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Pushes due job-control intents without reading engine runtime results.
 */
@Component
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "lakehouse-flow.job-control-intent-delivery.publisher",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class JobControlIntentDeliveryScanner {

    private final JobControlIntentDeliveryService deliveryService;
    private final List<SchedulingIntentPublisher> publishers;
    private final SchedulingIntentDeliveryMetrics deliveryMetrics;

    private final String generatedOwner = "job-control-scheduler-" + UUID.randomUUID();

    @Value("${lakehouse-flow.job-control-intent-delivery.publisher.owner:}")
    private String configuredOwner;

    @Value("${lakehouse-flow.job-control-intent-delivery.publisher.batch-size:100}")
    private Integer batchSize;

    @Value("${lakehouse-flow.job-control-intent-delivery.publisher.claim-lease:PT30S}")
    private Duration claimLease;

    @Value("${lakehouse-flow.job-control-intent-delivery.publisher.max-attempts:8}")
    private int maxAttempts;

    @Value("${lakehouse-flow.job-control-intent-delivery.publisher.initial-backoff:PT1S}")
    private Duration initialBackoff;

    @Value("${lakehouse-flow.job-control-intent-delivery.publisher.maximum-backoff:PT5M}")
    private Duration maximumBackoff;

    /** Claim and publish one bounded batch of job-control transport attempts. */
    @Scheduled(fixedDelayString = "${lakehouse-flow.job-control-intent-delivery.publisher.fixed-delay-ms:1000}")
    public void publishDueDeliveries() {
        for (SchedulingIntentPublication publication : deliveryService.claimDueDeliveries(
                effectiveOwner(), batchSize, claimLease)) {
            publish(publication);
        }
    }

    /** Push one claimed control instruction and persist only transport evidence. */
    private void publish(SchedulingIntentPublication publication) {
        long startedAt = System.nanoTime();
        try {
            SchedulingIntentPublisher publisher = publishers.stream()
                    .filter(candidate -> publication.channel().equals(candidate.channel()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No intent publisher configured for channel " + publication.channel()));
            publisher.publish(publication);
            boolean recorded = deliveryService.recordPublished(
                    publication.deliveryId(), publication.claimToken());
            String outcome = recorded ? "published" : "stale_acknowledgement";
            deliveryMetrics.recordAttempt(
                    publication,
                    outcome,
                    Duration.ofNanos(System.nanoTime() - startedAt));
            if (!recorded) {
                log.warn("Ignored stale job-control publication acknowledgement for delivery {}",
                        publication.deliveryId());
            }
        } catch (RuntimeException exception) {
            SchedulingIntentDeliveryFailureResult result = deliveryService.recordFailure(
                    publication.deliveryId(),
                    publication.claimToken(),
                    errorMessage(exception),
                    maxAttempts,
                    initialBackoff,
                    maximumBackoff);
            deliveryMetrics.recordAttempt(
                    publication,
                    result.name().toLowerCase(java.util.Locale.ROOT),
                    Duration.ofNanos(System.nanoTime() - startedAt));
            log.warn("Job-control intent delivery {} failed: {}",
                    publication.deliveryId(), errorMessage(exception));
        }
    }

    /** Resolve configured process identity or a generated per-process fallback. */
    private String effectiveOwner() {
        return configuredOwner == null || configuredOwner.isBlank()
                ? generatedOwner
                : configuredOwner.trim();
    }

    /** Produce one bounded transport-only failure description. */
    private String errorMessage(RuntimeException exception) {
        return exception.getMessage() == null || exception.getMessage().isBlank()
                ? exception.getClass().getSimpleName()
                : exception.getMessage();
    }
}
