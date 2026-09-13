package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotIds;
import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.LakehouseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Service for managing asset state and ensuring monotonic updates.
 *
 * Key responsibility: project table and partition state while ensuring:
 * - Newer snapshots always overwrite older ones
 * - Watermarks move forward monotonically
 * - Schema identity follows the snapshot that established the current table state
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
        return updateAssetStateForScope(event, normalizePartition(event.getPartitionName()));
    }

    /**
     * Project one table snapshot into table-level and changed-partition states.
     *
     * The table snapshot remains globally monotonic, while each partition gets
     * an independent AssetState key. This prevents a historical backfill
     * partition from satisfying a current-date partition dependency.
     *
     * @param event snapshot observation containing optional changedPartitions
     * @return updated table state followed by changed partition states
     */
    public List<AssetState> projectAssetStatesFromEvent(LakehouseEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("lakehouse event is required");
        }
        List<AssetState> states = new ArrayList<>();
        states.add(updateAssetStateForScope(event, null));
        changedPartitions(event).forEach(partition ->
                states.add(updateAssetStateForScope(event, partition)));
        return List.copyOf(states);
    }

    /**
     * Update or create one table or partition projection.
     *
     * @param event source snapshot observation
     * @param partitionName projection partition, or null for table scope
     * @return persisted or unchanged state
     */
    private AssetState updateAssetStateForScope(LakehouseEvent event, String partitionName) {
        if (event == null) {
            throw new IllegalArgumentException("lakehouse event is required");
        }
        String assetKey = buildAssetKey(event, partitionName);

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
                    .partitionName(partitionName)
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
     * Collect explicit and manifest-derived changed partitions without duplicates.
     *
     * @param event source snapshot observation
     * @return normalized changed partition names in source order
     */
    private Set<String> changedPartitions(LakehouseEvent event) {
        Set<String> partitions = new LinkedHashSet<>();
        addPartition(partitions, event.getPartitionName());
        if (event.getPayloadJson() == null) {
            return partitions;
        }
        Object rawPartitions = event.getPayloadJson().get("changedPartitions");
        if (rawPartitions instanceof Collection<?> changed) {
            changed.forEach(partition -> addPartition(partitions, String.valueOf(partition)));
        }
        return partitions;
    }

    /**
     * Add one nonblank normalized partition to a projection set.
     *
     * @param partitions target partition set
     * @param candidate candidate partition text
     */
    private void addPartition(Set<String> partitions, String candidate) {
        String normalized = normalizePartition(candidate);
        if (normalized != null) {
            partitions.add(normalized);
        }
    }

    /**
     * Normalize an optional partition name.
     *
     * @param partitionName source partition name
     * @return trimmed partition name, or null when blank
     */
    private String normalizePartition(String partitionName) {
        return partitionName == null || partitionName.isBlank() ? null : partitionName.trim();
    }

    /**
     * Check if event is newer than current state
     */
    private boolean isEventNewer(AssetState state, LakehouseEvent event) {
        return isIdentifierAfter(event.getSnapshotId(), state.getLatestSnapshotId())
                || event.getSnapshotId() == null
                        && (isTimeAfter(event.getWatermark(), state.getLatestWatermark())
                                || isTimeAfter(event.getCommitTime(), state.getLatestCommitTime()));
    }

    /**
     * Update asset state fields from event
     */
    private void updateAssetStateFields(AssetState state, LakehouseEvent event) {
        boolean snapshotAdvanced =
                isIdentifierAfter(event.getSnapshotId(), state.getLatestSnapshotId());
        if (snapshotAdvanced) {
            state.setLatestSnapshotId(event.getSnapshotId());
            if (event.getSchemaId() != null) {
                state.setLatestSchemaId(event.getSchemaId());
            }
        } else if (event.getSnapshotId() == null && event.getSchemaId() != null) {
            state.setLatestSchemaId(event.getSchemaId());
        }
        if (isTimeAfter(event.getWatermark(), state.getLatestWatermark())) {
            state.setLatestWatermark(event.getWatermark());
        }
        if (isTimeAfter(event.getCommitTime(), state.getLatestCommitTime())) {
            state.setLatestCommitTime(event.getCommitTime());
        }
        // Version is auto-incremented by @Version
    }

    /**
     * Compare nullable snapshot-like identifiers without allowing state regression.
     *
     * @param candidate newly observed identifier
     * @param current currently projected identifier
     * @return true when the candidate establishes or advances the identifier
     */
    private boolean isIdentifierAfter(String candidate, String current) {
        return candidate != null && (current == null || SnapshotIds.isAfter(candidate, current));
    }

    /**
     * Compare nullable timestamps without allowing state regression.
     *
     * @param candidate newly observed timestamp
     * @param current currently projected timestamp
     * @return true when the candidate establishes or advances the timestamp
     */
    private boolean isTimeAfter(LocalDateTime candidate, LocalDateTime current) {
        return candidate != null && (current == null || candidate.isAfter(current));
    }

    /**
     * Build asset key from event
     */
    private String buildAssetKey(LakehouseEvent event, String partitionName) {
        if (partitionName != null) {
            return String.format("%s.%s.%s.%s",
                    event.getCatalogName(),
                    event.getDatabaseName(),
                    event.getTableName(),
                    partitionName);
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
