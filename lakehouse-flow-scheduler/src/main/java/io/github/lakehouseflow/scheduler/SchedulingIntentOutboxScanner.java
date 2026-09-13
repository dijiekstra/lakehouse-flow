package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.service.SchedulingIntentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically commits ready scheduling decisions to the durable intent outbox.
 *
 * This scanner is owned by Lakehouse Flow. It creates the immutable intent and
 * selected route; a separate delivery scanner pushes external routes. Neither
 * scanner receives or infers external execution results.
 */
@Component
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "lakehouse-flow.scheduling-intent-outbox",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class SchedulingIntentOutboxScanner {

    private final SchedulingIntentService schedulingIntentService;

    @Value("${lakehouse-flow.scheduling-intent-outbox.batch-size:100}")
    private Integer batchSize;

    /**
     * Commit one bounded batch of ready task decisions.
     */
    @Scheduled(fixedDelayString = "${lakehouse-flow.scheduling-intent-outbox.fixed-delay-ms:1000}")
    public void publishReadyIntents() {
        int published = schedulingIntentService.publishReadyTaskIntents(batchSize).size();
        if (published > 0) {
            log.info("Committed {} scheduling intents to the durable outbox", published);
        }
    }
}
