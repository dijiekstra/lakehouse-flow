package io.github.lakehouseflow.integration.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically scans every configured lakehouse snapshot source.
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        name = "lakehouse-flow.snapshot-sources.scanner.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class LakehouseSnapshotScanner {

    private final EventIngestionService eventIngestionService;

    /**
     * Trigger one metadata-only scan across all enabled lake integrations.
     */
    @Scheduled(
            fixedDelayString = "${lakehouse-flow.snapshot-sources.scanner.fixed-delay-ms:10000}",
            initialDelayString = "${lakehouse-flow.snapshot-sources.scanner.initial-delay-ms:5000}")
    public void scanSnapshots() {
        try {
            int ingestedCount = eventIngestionService.ingestAllSources();
            if (ingestedCount > 0) {
                log.info("Lakehouse snapshot scan completed: {} events ingested", ingestedCount);
            } else {
                log.debug("Lakehouse snapshot scan completed: no new events");
            }
        } catch (RuntimeException e) {
            log.error("Lakehouse snapshot scan failed; the next scan will retry: {}", e.getMessage(), e);
        }
    }
}
