package io.github.lakehouseflow.integration.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Paimon Snapshot Scanner - Scheduled task that triggers event ingestion from Paimon.
 *
 * Runs every 10 seconds by default to scan and ingest new Paimon snapshots.
 * Can be disabled via application property: lakehouse.event-scanner.paimon.enabled=false
 *
 * This is part of the Event Ingestion Loop in the scheduler architecture.
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        name = "lakehouse.event-scanner.paimon.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class PaimonSnapshotScanner {

    private final EventIngestionService eventIngestionService;

    /**
     * Scan and ingest Paimon snapshots.
     *
     * Scheduled to run every 10 seconds (configurable via properties).
     * Configured properties:
     * - lakehouse.event-scanner.paimon.enabled (true/false)
     * - lakehouse.event-scanner.paimon.interval (milliseconds, default 10000)
     *
     * Error handling: Log errors but don't throw; next scan will retry.
     */
    @Scheduled(
            fixedDelayString = "${lakehouse.event-scanner.paimon.interval:10000}",
            initialDelayString = "${lakehouse.event-scanner.paimon.initial-delay:5000}"
    )
    public void scanSnapshots() {
        try {
            log.debug("Starting Paimon snapshot scan");

            int ingestedCount = eventIngestionService.ingestFromPaimon();

            if (ingestedCount > 0) {
                log.info("Paimon snapshot scan completed: {} events ingested", ingestedCount);
            } else {
                log.debug("Paimon snapshot scan completed: no new events");
            }

        } catch (Exception e) {
            log.error("Error during Paimon snapshot scan: {}", e.getMessage(), e);
            // Do not throw; allow next scheduled execution to retry
        }
    }
}
