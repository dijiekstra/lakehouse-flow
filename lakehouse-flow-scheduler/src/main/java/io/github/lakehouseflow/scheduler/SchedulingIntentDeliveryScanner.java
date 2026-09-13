package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.service.SchedulingIntentDeliveryService;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublisher;
import io.github.lakehouseflow.service.delivery.SchedulingIntentDeliveryFailureResult;
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
 * Claims and pushes due HTTP or MQ scheduling intent deliveries.
 *
 * Publication happens outside the claim transaction. Fenced completion prevents
 * a slow publisher from overwriting a newer attempt after its lease expires.
 */
@Component
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "lakehouse-flow.scheduling-intent-delivery.publisher",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class SchedulingIntentDeliveryScanner {

    private final SchedulingIntentDeliveryService schedulingIntentDeliveryService;
    private final List<SchedulingIntentPublisher> publishers;
    private final SchedulingIntentDeliveryMetrics deliveryMetrics;

    private final String generatedOwner = "scheduler-" + UUID.randomUUID();

    @Value("${lakehouse-flow.scheduling-intent-delivery.publisher.owner:}")
    private String configuredOwner;

    @Value("${lakehouse-flow.scheduling-intent-delivery.publisher.batch-size:100}")
    private Integer batchSize;

    @Value("${lakehouse-flow.scheduling-intent-delivery.publisher.claim-lease:PT30S}")
    private Duration claimLease;

    @Value("${lakehouse-flow.scheduling-intent-delivery.publisher.max-attempts:8}")
    private int maxAttempts;

    @Value("${lakehouse-flow.scheduling-intent-delivery.publisher.initial-backoff:PT1S}")
    private Duration initialBackoff;

    @Value("${lakehouse-flow.scheduling-intent-delivery.publisher.maximum-backoff:PT5M}")
    private Duration maximumBackoff;

    /** Claim and publish one bounded batch of due transport attempts. */
    @Scheduled(fixedDelayString = "${lakehouse-flow.scheduling-intent-delivery.publisher.fixed-delay-ms:1000}")
    public void publishDueDeliveries() {
        List<SchedulingIntentPublication> publications = schedulingIntentDeliveryService.claimDueDeliveries(
                effectiveOwner(),
                batchSize,
                claimLease);
        for (SchedulingIntentPublication publication : publications) {
            publish(publication);
        }
    }

    /** Publish one claim and persist only transport acknowledgement or failure. */
    private void publish(SchedulingIntentPublication publication) {
        long startedAt = System.nanoTime();
        try {
            SchedulingIntentPublisher publisher = publishers.stream()
                    .filter(candidate -> publication.channel().equals(candidate.channel()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No scheduling intent publisher configured for channel " + publication.channel()));
            publisher.publish(publication);
            boolean recorded = schedulingIntentDeliveryService.recordPublished(
                    publication.deliveryId(),
                    publication.claimToken());
            if (!recorded) {
                deliveryMetrics.recordAttempt(
                        publication,
                        "stale_acknowledgement",
                        Duration.ofNanos(System.nanoTime() - startedAt));
                log.warn("Ignored stale scheduling intent publication acknowledgement for delivery {}",
                        publication.deliveryId());
            } else {
                deliveryMetrics.recordAttempt(
                        publication,
                        "published",
                        Duration.ofNanos(System.nanoTime() - startedAt));
            }
        } catch (RuntimeException e) {
            SchedulingIntentDeliveryFailureResult result = schedulingIntentDeliveryService.recordFailure(
                    publication.deliveryId(),
                    publication.claimToken(),
                    errorMessage(e),
                    maxAttempts,
                    initialBackoff,
                    maximumBackoff);
            deliveryMetrics.recordAttempt(
                    publication,
                    result.name().toLowerCase(java.util.Locale.ROOT),
                    Duration.ofNanos(System.nanoTime() - startedAt));
            if (result != SchedulingIntentDeliveryFailureResult.STALE) {
                log.warn("Scheduling intent delivery {} failed: {}", publication.deliveryId(), errorMessage(e));
            } else {
                log.warn("Ignored stale scheduling intent delivery failure for {}", publication.deliveryId());
            }
        }
    }

    /** Resolve the stable configured owner or this process's generated identity. */
    private String effectiveOwner() {
        return configuredOwner == null || configuredOwner.isBlank()
                ? generatedOwner
                : configuredOwner.trim();
    }

    /** Produce a bounded transport-only failure description. */
    private String errorMessage(RuntimeException exception) {
        return exception.getMessage() == null || exception.getMessage().isBlank()
                ? exception.getClass().getSimpleName()
                : exception.getMessage();
    }
}
