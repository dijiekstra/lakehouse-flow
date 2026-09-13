package io.github.lakehouseflow.integration.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Periodically reconciles snapshot sources and applies bounded metadata compensation.
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        name = "lakehouse-flow.snapshot-sources.reconciliation.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class SnapshotSourceReconciliationScanner {

    private final SnapshotSourceReconciliationService reconciliationService;

    /**
     * Reconcile all configured sources without manufacturing or skipping snapshot evidence.
     */
    @Scheduled(
            fixedDelayString = "${lakehouse-flow.snapshot-sources.reconciliation.fixed-delay-ms:60000}",
            initialDelayString = "${lakehouse-flow.snapshot-sources.reconciliation.initial-delay-ms:15000}")
    public void reconcileSources() {
        try {
            List<SnapshotSourceReconciliation> results =
                    reconciliationService.reconcileAndRepairAllSources();
            results.stream()
                    .filter(result -> result.outcome() != SnapshotSourceReconciliationOutcome.HEALTHY)
                    .forEach(result -> log.warn(
                            "Snapshot source reconciliation {}/{}: outcome={}, offset={}, projection={}, detail={}",
                            result.identity().sourceType(),
                            result.identity().sourceName(),
                            result.outcome(),
                            result.offsetStatus(),
                            result.projectionStatus(),
                            result.detail()));
        } catch (RuntimeException e) {
            log.error("Snapshot source reconciliation scan failed: {}", e.getMessage(), e);
        }
    }
}
