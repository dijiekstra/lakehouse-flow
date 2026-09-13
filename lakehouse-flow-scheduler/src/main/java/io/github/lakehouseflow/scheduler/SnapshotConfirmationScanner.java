package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.model.SnapshotConfirmationResult;
import io.github.lakehouseflow.service.SnapshotConfirmationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Periodically confirms scheduled tasks from target asset snapshot progress.
 */
@Component
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "lakehouse-flow.snapshot-confirmation",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class SnapshotConfirmationScanner {

    private final SnapshotConfirmationService snapshotConfirmationService;

    @Value("${lakehouse-flow.snapshot-confirmation.timeout:PT1H}")
    private Duration confirmationTimeout;

    /**
     * Scan scheduled tasks and confirm them from target snapshot progress.
     */
    @Scheduled(fixedDelayString = "${lakehouse-flow.snapshot-confirmation.fixed-delay-ms:30000}")
    public void scanScheduledTasks() {
        List<SnapshotConfirmationResult> results = snapshotConfirmationService.checkScheduledTasks(
                confirmationTimeout);
        if (results.isEmpty()) {
            return;
        }

        long confirmed = results.stream()
                .filter(SnapshotConfirmationResult::snapshotAdvanced)
                .count();
        long expired = results.stream()
                .filter(SnapshotConfirmationResult::confirmationExpired)
                .count();
        long waiting = results.size() - confirmed - expired;

        log.info("Snapshot confirmation scan checked {} scheduled tasks: confirmed={}, waiting={}, expired={}",
                results.size(), confirmed, waiting, expired);
    }
}
