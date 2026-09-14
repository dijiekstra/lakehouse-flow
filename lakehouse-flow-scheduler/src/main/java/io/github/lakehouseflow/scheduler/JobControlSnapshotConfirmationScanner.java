package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.service.JobControlSnapshotConfirmationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically checks writer-generation snapshots independently from task confirmation.
 */
@Component
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "lakehouse-flow.job-control-snapshot-confirmation",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class JobControlSnapshotConfirmationScanner {

    private final JobControlSnapshotConfirmationService confirmationService;

    /** Reconcile all waiting job-control intents against durable snapshot evidence. */
    @Scheduled(fixedDelayString = "${lakehouse-flow.job-control-snapshot-confirmation.fixed-delay-ms:30000}")
    public void checkWaitingIntents() {
        int checked = confirmationService.checkWaitingIntents().size();
        if (checked > 0) {
            log.debug("Checked {} waiting job-control snapshot intent(s)", checked);
        }
    }
}
