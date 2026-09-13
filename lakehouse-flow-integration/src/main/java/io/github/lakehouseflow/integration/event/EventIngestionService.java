package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.integration.source.LakehouseSnapshot;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotEventMapper;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSource;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSourceRegistry;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.model.EventConsumerOffset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.time.Duration;

/**
 * Orchestrates snapshot ingestion for every configured lakehouse format.
 *
 * <p>Each adapter owns metadata discovery and offset ordering. This service owns durable offset
 * lookup, event mapping, and the per-snapshot transaction boundary shared by Paimon, Iceberg,
 * Hudi, and future integrations.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventIngestionService {

    private final LakehouseSnapshotSourceRegistry sourceRegistry;
    private final EventConsumerOffsetRepository eventConsumerOffsetRepository;
    private final LakehouseSnapshotEventMapper eventMapper;
    private final SnapshotIngestionTransactionService snapshotIngestionTransactionService;
    private final SnapshotSourceMetrics snapshotSourceMetrics;

    /**
     * Ingest every configured source while isolating failures between source tables.
     *
     * @return number of newly inserted lakehouse events
     */
    public int ingestAllSources() {
        int inserted = 0;
        for (LakehouseSnapshotSource source : sourceRegistry.sources()) {
            long startedAt = System.nanoTime();
            try {
                int sourceInserted = ingestSource(source);
                inserted += sourceInserted;
                snapshotSourceMetrics.recordScan(
                        source.identity(),
                        "success",
                        sourceInserted,
                        Duration.ofNanos(System.nanoTime() - startedAt));
            } catch (RuntimeException e) {
                LakehouseSourceIdentity identity = source.identity();
                snapshotSourceMetrics.recordScan(
                        identity,
                        "failure",
                        0,
                        Duration.ofNanos(System.nanoTime() - startedAt));
                log.error("Snapshot source scan failed for {}/{}: {}",
                        identity.sourceType(), identity.sourceName(), e.getMessage(), e);
            }
        }
        return inserted;
    }

    /**
     * Ingest one table source through its contiguous successful snapshot prefix.
     *
     * <p>If projection of one snapshot fails, later snapshots from the same source are not
     * processed. The failed offset remains uncommitted and is retried by the next scan.
     *
     * @param source configured lakehouse snapshot source
     * @return number of newly inserted events
     */
    public int ingestSource(LakehouseSnapshotSource source) {
        LakehouseSourceIdentity identity = source.identity();
        Comparator<String> offsetComparator = source.offsetComparator();
        Optional<EventConsumerOffset> lastOffset = eventConsumerOffsetRepository
                .findBySourceTypeAndSourceName(identity.sourceType(), identity.sourceName());
        String offsetExclusive = lastOffset.map(EventConsumerOffset::getOffsetValue).orElse(null);

        List<LakehouseSnapshot> snapshots = new ArrayList<>(source.scanAfter(offsetExclusive));
        snapshots.sort((left, right) -> offsetComparator.compare(
                left.sourceOffset(), right.sourceOffset()));
        int inserted = 0;
        for (LakehouseSnapshot snapshot : snapshots) {
            try {
                var event = eventMapper.map(identity, snapshot);
                var result = snapshotIngestionTransactionService.processSnapshot(
                        identity.sourceName(),
                        snapshot.sourceOffset(),
                        offsetComparator,
                        event);
                if (result.inserted()) {
                    inserted++;
                }
            } catch (RuntimeException e) {
                log.error("Snapshot projection failed for {}/{} at offset {}; later offsets will wait: {}",
                        identity.sourceType(), identity.sourceName(), snapshot.sourceOffset(),
                        e.getMessage(), e);
                break;
            }
        }
        return inserted;
    }

}
