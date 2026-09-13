package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer instrumentation for snapshot scans and source reconciliation.
 */
@Component
public class SnapshotSourceMetrics {

    private static final String METRIC_PREFIX = "lakehouse.flow.snapshot.source";

    private final MeterRegistry meterRegistry;
    private final ConcurrentMap<SourceKey, SourceGauges> gauges = new ConcurrentHashMap<>();

    /**
     * Create source metrics backed by the application meter registry.
     *
     * @param meterRegistry application meter registry
     */
    public SnapshotSourceMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Record one source metadata scan and the number of newly inserted events.
     *
     * @param identity scanned source identity
     * @param outcome scan outcome such as success or failure
     * @param insertedEvents newly inserted event count
     * @param duration metadata scan duration
     */
    public void recordScan(
            LakehouseSourceIdentity identity,
            String outcome,
            int insertedEvents,
            Duration duration) {
        String[] tags = tags(identity, "outcome", outcome);
        Counter.builder(METRIC_PREFIX + ".scans")
                .tags(tags)
                .register(meterRegistry)
                .increment();
        Counter.builder(METRIC_PREFIX + ".events.ingested")
                .tags(tags(identity))
                .register(meterRegistry)
                .increment(insertedEvents);
        Timer.builder(METRIC_PREFIX + ".scan.duration")
                .tags(tags)
                .register(meterRegistry)
                .record(duration);
    }

    /**
     * Record one reconciliation result and refresh its low-cardinality source gauges.
     *
     * @param reconciliation latest source reconciliation result
     */
    public void recordReconciliation(SnapshotSourceReconciliation reconciliation) {
        LakehouseSourceIdentity identity = reconciliation.identity();
        Counter.builder(METRIC_PREFIX + ".reconciliations")
                .tags(tags(
                        identity,
                        "outcome", reconciliation.outcome().name(),
                        "offset_status", reconciliation.offsetStatus().name(),
                        "projection_status", reconciliation.projectionStatus().name()))
                .register(meterRegistry)
                .increment();

        SourceGauges sourceGauges = gauges.computeIfAbsent(
                new SourceKey(identity.sourceType(), identity.sourceName()),
                ignored -> registerGauges(identity));
        sourceGauges.pendingOffsets().set(
                reconciliation.pendingOffsetCount() == null ? -1L : reconciliation.pendingOffsetCount());
        sourceGauges.healthy().set(
                reconciliation.outcome() == SnapshotSourceReconciliationOutcome.HEALTHY ? 1L : 0L);
        sourceGauges.retentionGap().set(
                reconciliation.offsetStatus() == SnapshotSourceOffsetStatus.RETENTION_GAP ? 1L : 0L);
        sourceGauges.projectionDrift().set(
                reconciliation.projectionStatus() == SnapshotProjectionStatus.CONSISTENT
                        || reconciliation.projectionStatus() == SnapshotProjectionStatus.NOT_APPLICABLE
                        ? 0L
                        : 1L);
    }

    /** Register gauges for one bounded configured source identity. */
    private SourceGauges registerGauges(LakehouseSourceIdentity identity) {
        SourceGauges values = new SourceGauges(
                new AtomicLong(-1L),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong());
        registerGauge("offsets.pending", identity, values.pendingOffsets());
        registerGauge("healthy", identity, values.healthy());
        registerGauge("retention.gap", identity, values.retentionGap());
        registerGauge("projection.inconsistent", identity, values.projectionDrift());
        return values;
    }

    /** Register one source-tagged long gauge. */
    private void registerGauge(String suffix, LakehouseSourceIdentity identity, AtomicLong value) {
        Gauge.builder(METRIC_PREFIX + "." + suffix, value, AtomicLong::get)
                .tags(tags(identity))
                .register(meterRegistry);
    }

    /** Build common source tags with optional additional key-value pairs. */
    private String[] tags(LakehouseSourceIdentity identity, String... additionalTags) {
        String[] tags = new String[4 + additionalTags.length];
        tags[0] = "source_type";
        tags[1] = identity.sourceType();
        tags[2] = "source_name";
        tags[3] = identity.sourceName();
        System.arraycopy(additionalTags, 0, tags, 4, additionalTags.length);
        return tags;
    }

    /** Stable key for one configured source's gauge holders. */
    private record SourceKey(String sourceType, String sourceName) {
    }

    /** Mutable gauge holders refreshed by reconciliation. */
    private record SourceGauges(
            AtomicLong pendingOffsets,
            AtomicLong healthy,
            AtomicLong retentionGap,
            AtomicLong projectionDrift) {
    }
}
