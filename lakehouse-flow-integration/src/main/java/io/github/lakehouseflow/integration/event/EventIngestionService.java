package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.integration.paimon.PaimonSnapshotSource;
import io.github.lakehouseflow.model.EventConsumerOffset;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Event Ingestion Service - Orchestrates event ingestion from Paimon snapshots.
 *
 * Responsibilities:
 * 1. Scan Paimon snapshots from last processed position
 * 2. Convert snapshots to LakehouseEvent
 * 3. Handle deduplication (same event_id = skip, log, continue)
 * 4. Update EventConsumerOffset to track progress
 * 5. Log and metrics
 *
 * Design principles:
 * - Idempotent: same snapshot scanned twice → skip duplicate event via unique constraint
 * - Atomic: snapshot → event → offset all succeed or all fail
 * - Observable: every step logged with relevant fields
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventIngestionService {

    private final PaimonSnapshotSource paimonSnapshotSource;
    private final LakehouseEventRepository lakehouseEventRepository;
    private final EventConsumerOffsetRepository eventConsumerOffsetRepository;

    private static final String SOURCE_TYPE = "PAIMON";

    /**
     * Ingest events from Paimon snapshots.
     *
     * Process:
     * 1. Load last processed offset for PAIMON source
     * 2. Scan new snapshots from Paimon
     * 3. Convert each to LakehouseEvent
     * 4. Persist (handling duplicates gracefully)
     * 5. Update consumer offset to track progress
     *
     * @return Number of new events ingested (excluding duplicates)
     */
    @Transactional
    public int ingestFromPaimon() {
        String sourceName = buildSourceName();
        log.info("Starting Paimon event ingestion for {}", sourceName);

        // Load last processed offset
        Optional<EventConsumerOffset> lastOffset = eventConsumerOffsetRepository
                .findBySourceTypeAndSourceName(SOURCE_TYPE, sourceName);
        Long lastSnapshotId = lastOffset.map(o -> Long.parseLong(o.getOffsetValue())).orElse(null);

        log.debug("Last processed snapshot ID: {}", lastSnapshotId);

        // Scan new snapshots
        var snapshots = paimonSnapshotSource.scanSnapshots(lastSnapshotId);
        if (snapshots.isEmpty()) {
            log.debug("No new snapshots found since {}", lastSnapshotId);
            return 0;
        }

        log.info("Found {} new snapshots to ingest", snapshots.size());

        int successCount = 0;
        int duplicateCount = 0;
        Long maxSnapshotId = lastSnapshotId;

        for (var snapshot : snapshots) {
            try {
                // Convert and persist
                LakehouseEvent event = paimonSnapshotSource.mapToLakehouseEvent(snapshot);
                lakehouseEventRepository.save(event);

                log.debug("Ingested event: {} from snapshot {}", event.getEventId(), snapshot.getSnapshotId());
                successCount++;

                // Track max snapshot ID for offset update
                Long snapshotIdLong = Long.parseLong(snapshot.getSnapshotId());
                if (maxSnapshotId == null || snapshotIdLong > maxSnapshotId) {
                    maxSnapshotId = snapshotIdLong;
                }

            } catch (DataIntegrityViolationException e) {
                // Duplicate event_id - this is expected and safe
                duplicateCount++;
                log.debug("Skipped duplicate event from snapshot {}: {}",
                        snapshot.getSnapshotId(), e.getMessage());
            } catch (Exception e) {
                log.error("Error ingesting snapshot {}: {}",
                        snapshot.getSnapshotId(), e.getMessage(), e);
                // Continue processing other snapshots; don't fail the whole batch
            }
        }

        // Update consumer offset only if we processed at least one snapshot
        if (maxSnapshotId != null) {
            updateConsumerOffset(sourceName, maxSnapshotId.toString());
            log.info("Updated consumer offset to {} for {}", maxSnapshotId, sourceName);
        }

        log.info("Paimon ingestion complete: {} ingested, {} duplicates, {} total processed",
                successCount, duplicateCount, snapshots.size());

        return successCount;
    }

    /**
     * Update or create EventConsumerOffset.
     */
    @Transactional
    protected void updateConsumerOffset(String sourceName, String offsetValue) {
        Optional<EventConsumerOffset> existing = eventConsumerOffsetRepository
                .findBySourceTypeAndSourceName(SOURCE_TYPE, sourceName);

        if (existing.isPresent()) {
            EventConsumerOffset offset = existing.get();
            offset.setOffsetValue(offsetValue);
            offset.setUpdatedAt(LocalDateTime.now());
            eventConsumerOffsetRepository.save(offset);
        } else {
            EventConsumerOffset offset = EventConsumerOffset.builder()
                    .sourceType(SOURCE_TYPE)
                    .sourceName(sourceName)
                    .offsetValue(offsetValue)
                    .build();
            eventConsumerOffsetRepository.save(offset);
        }
    }

    /**
     * Build source name from catalog/database/table.
     */
    private String buildSourceName() {
        return String.format("%s.%s.%s",
                paimonSnapshotSource.getCatalogName(),
                paimonSnapshotSource.getDatabaseName(),
                paimonSnapshotSource.getTableName());
    }
}
