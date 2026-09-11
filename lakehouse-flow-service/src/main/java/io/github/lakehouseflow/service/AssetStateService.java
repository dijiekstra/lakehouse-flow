package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.LakehouseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Service for managing asset state and ensuring monotonic updates.
 *
 * Key responsibility: Update asset state based on events while ensuring:
 * - Newer snapshots always overwrite older ones
 * - Watermarks move forward monotonically
 * - Version is incremented for optimistic locking
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class AssetStateService {

    private final AssetStateRepository assetStateRepository;

    /**
     * Update or create asset state from an event.
     *
     * Logic:
     * 1. Check if snapshot/watermark is newer than current
     * 2. Only update if conditions are satisfied
     * 3. Increment version for concurrency control
     *
     * Returns: AssetState after update (or unchanged if event is stale)
     */
    public AssetState updateAssetStateFromEvent(LakehouseEvent event) {
        String assetKey = buildAssetKey(event);

        // Find existing asset state
        Optional<AssetState> existing = assetStateRepository.findByAssetKey(assetKey);

        if (existing.isPresent()) {
            AssetState state = existing.get();

            // Check if this event is newer (monotonic check)
            if (isEventNewer(state, event)) {
                log.debug("Updating asset state for {} from event {}", assetKey, event.getEventId());
                updateAssetStateFields(state, event);
                return assetStateRepository.save(state);
            } else {
                log.debug("Ignoring stale event {} for asset {}", event.getEventId(), assetKey);
                return state;
            }
        } else {
            // Create new asset state
            log.info("Creating new asset state for {} from event {}", assetKey, event.getEventId());
            AssetState newState = AssetState.builder()
                    .assetKey(assetKey)
                    .assetType("TABLE")
                    .catalogName(event.getCatalogName())
                    .databaseName(event.getDatabaseName())
                    .tableName(event.getTableName())
                    .partitionName(event.getPartitionName())
                    .latestSnapshotId(event.getSnapshotId())
                    .latestSchemaId(event.getSchemaId())
                    .latestWatermark(event.getWatermark())
                    .latestCommitTime(event.getCommitTime())
                    .qualityStatus("UNKNOWN")
                    .schemaStatus("UNKNOWN")
                    .backfillStatus("NONE")
                    .readinessStatus("UNKNOWN")
                    .createdAt(LocalDateTime.now())
                    .updatedAt(LocalDateTime.now())
                    .build();
            return assetStateRepository.save(newState);
        }
    }

    /**
     * Check if event is newer than current state
     */
    private boolean isEventNewer(AssetState state, LakehouseEvent event) {
        // If event has snapshot ID, compare lexicographically
        if (event.getSnapshotId() != null && state.getLatestSnapshotId() != null) {
            int comparison = event.getSnapshotId().compareTo(state.getLatestSnapshotId());
            if (comparison > 0) return true;
            if (comparison < 0) return false;
        }

        // If event has watermark, compare timestamps
        if (event.getWatermark() != null && state.getLatestWatermark() != null) {
            return event.getWatermark().isAfter(state.getLatestWatermark());
        }

        // Default: use commit time
        if (event.getCommitTime() != null && state.getLatestCommitTime() != null) {
            return event.getCommitTime().isAfter(state.getLatestCommitTime());
        }

        return false;
    }

    /**
     * Update asset state fields from event
     */
    private void updateAssetStateFields(AssetState state, LakehouseEvent event) {
        if (event.getSnapshotId() != null) {
            state.setLatestSnapshotId(event.getSnapshotId());
        }
        if (event.getSchemaId() != null) {
            state.setLatestSchemaId(event.getSchemaId());
        }
        if (event.getWatermark() != null) {
            state.setLatestWatermark(event.getWatermark());
        }
        if (event.getCommitTime() != null) {
            state.setLatestCommitTime(event.getCommitTime());
        }
        // Version is auto-incremented by @Version
    }

    /**
     * Build asset key from event
     */
    private String buildAssetKey(LakehouseEvent event) {
        if (event.getPartitionName() != null) {
            return String.format("%s.%s.%s.%s",
                    event.getCatalogName(),
                    event.getDatabaseName(),
                    event.getTableName(),
                    event.getPartitionName());
        }
        return String.format("%s.%s.%s",
                event.getCatalogName(),
                event.getDatabaseName(),
                event.getTableName());
    }

    /**
     * Get asset state by key
     */
    @Transactional(readOnly = true)
    public Optional<AssetState> getAssetState(String assetKey) {
        return assetStateRepository.findByAssetKey(assetKey);
    }

    /**
     * Mark asset as ready
     */
    public AssetState markAsReady(String assetKey) {
        Optional<AssetState> existing = assetStateRepository.findByAssetKey(assetKey);
        if (existing.isPresent()) {
            AssetState state = existing.get();
            state.setReadinessStatus("READY");
            log.info("Marked asset {} as READY", assetKey);
            return assetStateRepository.save(state);
        }
        return null;
    }
}
