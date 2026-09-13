package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests snapshot scan counters and reconciliation gauges.
 */
class SnapshotSourceMetricsTest {

    private static final LakehouseSourceIdentity IDENTITY =
            new LakehouseSourceIdentity("PAIMON", "orders", "lake", "ods", "orders");

    /** Verify one source scan records count, inserted events, and duration. */
    @Test
    void recordScanPublishesMicrometerEvidence() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SnapshotSourceMetrics metrics = new SnapshotSourceMetrics(registry);

        metrics.recordScan(IDENTITY, "success", 2, Duration.ofMillis(10));

        assertEquals(1.0, registry.get("lakehouse.flow.snapshot.source.scans")
                .tags("source_type", "PAIMON", "source_name", "orders", "outcome", "success")
                .counter()
                .count());
        assertEquals(2.0, registry.get("lakehouse.flow.snapshot.source.events.ingested")
                .tags("source_type", "PAIMON", "source_name", "orders")
                .counter()
                .count());
    }

    /** Verify blocked reconciliation refreshes lag, health, and gap gauges. */
    @Test
    void recordReconciliationPublishesCurrentSourceState() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SnapshotSourceMetrics metrics = new SnapshotSourceMetrics(registry);
        SnapshotSourceReconciliation reconciliation = new SnapshotSourceReconciliation(
                IDENTITY,
                SnapshotSourceReconciliationOutcome.BLOCKED,
                SnapshotSourceOffsetStatus.RETENTION_GAP,
                SnapshotProjectionStatus.CONSISTENT,
                "8", "10", "12", "12", "8", "8", "8", "8", 3L,
                false, 0, "gap", LocalDateTime.now());

        metrics.recordReconciliation(reconciliation);

        assertEquals(3.0, gauge(registry, "offsets.pending"));
        assertEquals(0.0, gauge(registry, "healthy"));
        assertEquals(1.0, gauge(registry, "retention.gap"));
        assertEquals(0.0, gauge(registry, "projection.inconsistent"));
    }

    /** Read one source-tagged gauge. */
    private double gauge(SimpleMeterRegistry registry, String suffix) {
        return registry.get("lakehouse.flow.snapshot.source." + suffix)
                .tags("source_type", "PAIMON", "source_name", "orders")
                .gauge()
                .value();
    }
}
