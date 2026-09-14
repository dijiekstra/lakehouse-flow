package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.common.SnapshotIds;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.model.SchedulingIntent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Matches target snapshots to immutable Lakehouse Flow scheduling intents.
 *
 * Every snapshot still contributes to AssetState, but only a data commit with
 * the required intent properties and target partition can confirm one task.
 * This keeps external writes and maintenance commits from producing false
 * positives while preserving snapshot evidence as the sole result boundary.
 */
@Service
@RequiredArgsConstructor
public class SnapshotEvidenceService {

    private final LakehouseEventRepository lakehouseEventRepository;

    /**
     * Find a target snapshot attributable to one scheduling intent.
     *
     * @param intent immutable scheduling intent and baseline
     * @return satisfied only when a matching final data snapshot exists
     */
    @Transactional(readOnly = true)
    public EvaluationResult evaluateIntentProgress(SchedulingIntent intent) {
        AssetAddress target = requireIntentTarget(intent);
        List<LakehouseEvent> newerEvents = lakehouseEventRepository
                .findSnapshotEvidenceCandidates(
                        target.catalogName(),
                        target.databaseName(),
                        target.tableName(),
                        intent.getCreatedAt())
                .stream()
                .filter(event -> isAfterBaseline(event.getSnapshotId(), intent.getBaselineSnapshotId()))
                .toList();

        Optional<LakehouseEvent> matchingEvent = newerEvents.stream()
                .filter(this::isAcceptedDataCommit)
                .filter(event -> hasRequiredSnapshotProperties(event, intent))
                .filter(event -> changesExpectedPartition(event, target.partitionName()))
                .reduce(this::laterSnapshot);
        if (matchingEvent.isPresent()) {
            LakehouseEvent event = matchingEvent.get();
            return EvaluationResult.builder()
                    .satisfied(true)
                    .assetKey(intent.getTargetAssetKey())
                    .snapshotId(event.getSnapshotId())
                    .eventId(event.getEventId())
                    .description("Target snapshot " + event.getSnapshotId()
                            + " matched scheduling intent " + intent.getIntentKey())
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }

        String latestObservedSnapshotId = newerEvents.stream()
                .reduce(this::laterSnapshot)
                .map(LakehouseEvent::getSnapshotId)
                .orElse(intent.getBaselineSnapshotId());
        String waitingReason = newerEvents.isEmpty()
                ? "Waiting for a target snapshot attributed to intent " + intent.getIntentKey()
                : "Observed " + newerEvents.size() + " newer target snapshot(s), but none matched intent "
                        + intent.getIntentKey();
        return EvaluationResult.builder()
                .satisfied(false)
                .assetKey(intent.getTargetAssetKey())
                .snapshotId(latestObservedSnapshotId)
                .waitingReason(waitingReason)
                .description("Target snapshot attribution evidence is not available yet")
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Find a business-data snapshot attributable to one platform writer generation.
     *
     * @param intent immutable job-control intent and table baseline
     * @return satisfied only when the controlled writer epoch produced target data
     */
    @Transactional(readOnly = true)
    public EvaluationResult evaluateJobControlProgress(JobControlIntent intent) {
        AssetAddress target = requireJobControlTarget(intent);
        List<LakehouseEvent> newerEvents = lakehouseEventRepository
                .findSnapshotEvidenceCandidates(
                        target.catalogName(),
                        target.databaseName(),
                        target.tableName(),
                        intent.getCreatedAt())
                .stream()
                .filter(event -> isAfterBaseline(event.getSnapshotId(), intent.getBaselineSnapshotId()))
                .toList();
        Optional<LakehouseEvent> matchingEvent = newerEvents.stream()
                .filter(this::isAcceptedDataCommit)
                .filter(event -> hasRequiredWriterProperties(event, intent))
                .reduce(this::laterSnapshot);
        if (matchingEvent.isPresent()) {
            LakehouseEvent event = matchingEvent.get();
            return EvaluationResult.builder()
                    .satisfied(true)
                    .assetKey(intent.getTableAssetKey())
                    .snapshotId(event.getSnapshotId())
                    .eventId(event.getEventId())
                    .description("Target snapshot " + event.getSnapshotId()
                            + " matched writer generation " + intent.getWriterJobKey()
                            + "@" + intent.getWriterEpoch())
                    .evaluatedAt(System.currentTimeMillis())
                    .build();
        }
        String latestObservedSnapshotId = newerEvents.stream()
                .reduce(this::laterSnapshot)
                .map(LakehouseEvent::getSnapshotId)
                .orElse(intent.getBaselineSnapshotId());
        return EvaluationResult.builder()
                .satisfied(false)
                .assetKey(intent.getTableAssetKey())
                .snapshotId(latestObservedSnapshotId)
                .waitingReason("Waiting for target data snapshot attributed to writer generation "
                        + intent.getWriterJobKey() + "@" + intent.getWriterEpoch())
                .description("Writer generation snapshot attribution evidence is not available yet")
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Validate the immutable intent and parse its target asset address.
     *
     * @param intent scheduling intent to evaluate
     * @return parsed catalog, database, table, and optional partition
     */
    private AssetAddress requireIntentTarget(SchedulingIntent intent) {
        if (intent == null) {
            throw new IllegalArgumentException("scheduling intent is required");
        }
        if (isBlank(intent.getIntentKey())) {
            throw new IllegalArgumentException("scheduling intent key is required");
        }
        if (intent.getBizDate() == null) {
            throw new IllegalArgumentException("scheduling intent business date is required");
        }
        if (intent.getCreatedAt() == null) {
            throw new IllegalArgumentException("scheduling intent creation time is required");
        }
        if (isBlank(intent.getTargetAssetKey())) {
            throw new IllegalArgumentException("scheduling intent target asset is required");
        }
        if (isBlank(intent.getWriterJobKey()) || intent.getWriterEpoch() == null
                || intent.getWriterEpoch() <= 0) {
            throw new IllegalArgumentException("scheduling intent writer job key and positive epoch are required");
        }

        String[] parts = intent.getTargetAssetKey().trim().split("\\.", 4);
        if (parts.length < 3 || isBlank(parts[0]) || isBlank(parts[1]) || isBlank(parts[2])) {
            throw new IllegalArgumentException(
                    "target asset key must use catalog.database.table[.partition]");
        }
        return new AssetAddress(parts[0], parts[1], parts[2], parts.length == 4 ? parts[3] : null);
    }

    /** Validate and parse the physical target of one job-control intent. */
    private AssetAddress requireJobControlTarget(JobControlIntent intent) {
        if (intent == null) {
            throw new IllegalArgumentException("job control intent is required");
        }
        if (isBlank(intent.getIntentKey()) || isBlank(intent.getWriterJobKey())) {
            throw new IllegalArgumentException("job control intent and writer keys are required");
        }
        if (intent.getWriterEpoch() == null || intent.getWriterEpoch() <= 0) {
            throw new IllegalArgumentException("job control writer epoch must be positive");
        }
        if (intent.getCreatedAt() == null) {
            throw new IllegalArgumentException("job control intent creation time is required");
        }
        if (isBlank(intent.getTableAssetKey())) {
            throw new IllegalArgumentException("job control table asset is required");
        }
        String[] parts = intent.getTableAssetKey().trim().split("\\.", 4);
        if (parts.length != 3 || isBlank(parts[0]) || isBlank(parts[1]) || isBlank(parts[2])) {
            throw new IllegalArgumentException("job control table asset must use catalog.database.table");
        }
        return new AssetAddress(parts[0], parts[1], parts[2], null);
    }

    /**
     * Check whether an observed snapshot is newer than the frozen baseline.
     *
     * @param snapshotId observed snapshot id
     * @param baselineSnapshotId frozen snapshot id, or null
     * @return true when the event can be considered by this intent
     */
    private boolean isAfterBaseline(String snapshotId, String baselineSnapshotId) {
        if (isBlank(snapshotId)) {
            return false;
        }
        return isBlank(baselineSnapshotId) || SnapshotIds.isAfter(snapshotId, baselineSnapshotId);
    }

    /**
     * Accept only data-producing commits, excluding compaction and analysis.
     *
     * @param event candidate snapshot event
     * @return true when its source adapter classified it as target data output
     */
    private boolean isAcceptedDataCommit(LakehouseEvent event) {
        return SnapshotEventChangeClassifier.isDataChange(event);
    }

    /**
     * Match every required scheduling-intent property on a snapshot event.
     *
     * @param event candidate snapshot event
     * @param intent expected scheduling intent
     * @return true when the snapshot carries exact attribution and finality markers
     */
    private boolean hasRequiredSnapshotProperties(LakehouseEvent event, SchedulingIntent intent) {
        Map<?, ?> properties = snapshotProperties(event);
        return propertyEquals(properties,
                SnapshotEvidenceContract.SOURCE_PROPERTY,
                SnapshotEvidenceContract.INTENT_SOURCE)
                && propertyEquals(properties,
                        SnapshotEvidenceContract.INTENT_KEY_PROPERTY,
                        intent.getIntentKey())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY,
                        intent.getWriterJobKey())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY,
                        Long.toString(intent.getWriterEpoch()))
                && propertyEquals(properties,
                        SnapshotEvidenceContract.TARGET_ASSET_PROPERTY,
                        intent.getTargetAssetKey())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.BIZ_DATE_PROPERTY,
                        intent.getBizDate().toLocalDate().toString())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.FINAL_PROPERTY,
                        SnapshotEvidenceContract.FINAL_VALUE);
    }

    /** Match every required platform writer-generation property. */
    private boolean hasRequiredWriterProperties(LakehouseEvent event, JobControlIntent intent) {
        Map<?, ?> properties = snapshotProperties(event);
        return propertyEquals(properties,
                SnapshotEvidenceContract.SOURCE_PROPERTY,
                SnapshotEvidenceContract.INTENT_SOURCE)
                && propertyEquals(properties,
                        SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY,
                        intent.getIntentKey())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY,
                        intent.getWriterJobKey())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY,
                        Long.toString(intent.getWriterEpoch()));
    }

    /**
     * Read normalized snapshot properties from the durable raw-event payload.
     *
     * @param event candidate snapshot event
     * @return snapshot properties, or an empty map when the source omitted them
     */
    private Map<?, ?> snapshotProperties(LakehouseEvent event) {
        Map<String, Object> payload = event.getPayloadJson();
        if (payload == null) {
            return Map.of();
        }
        Object rawProperties = payload.get("snapshotProperties");
        return rawProperties instanceof Map<?, ?> properties ? properties : Map.of();
    }

    /**
     * Compare one snapshot property as text.
     *
     * @param properties observed snapshot property map
     * @param key required property key
     * @param expected expected property value
     * @return true when the observed value matches exactly
     */
    private boolean propertyEquals(Map<?, ?> properties, String key, String expected) {
        Object observed = properties.get(key);
        return observed != null && expected.equals(String.valueOf(observed));
    }

    /**
     * Verify that a partition-scoped intent changed its expected partition.
     *
     * Table-level intents do not require partition evidence. Partition-level
     * events may expose the partition directly or through changedPartitions
     * derived from the snapshot delta manifests.
     *
     * @param event candidate snapshot event
     * @param expectedPartition expected partition suffix, or null
     * @return true when partition evidence satisfies the intent scope
     */
    private boolean changesExpectedPartition(LakehouseEvent event, String expectedPartition) {
        if (isBlank(expectedPartition)) {
            return true;
        }
        if (expectedPartition.equals(event.getPartitionName())) {
            return true;
        }
        Map<String, Object> payload = event.getPayloadJson();
        if (payload == null) {
            return false;
        }
        Object rawPartitions = payload.get("changedPartitions");
        if (!(rawPartitions instanceof Collection<?> partitions)) {
            return false;
        }
        return partitions.stream().anyMatch(partition -> expectedPartition.equals(String.valueOf(partition)));
    }

    /**
     * Select the event with the later snapshot identifier.
     *
     * @param left first event
     * @param right second event
     * @return event with the later snapshot id
     */
    private LakehouseEvent laterSnapshot(LakehouseEvent left, LakehouseEvent right) {
        return SnapshotIds.isAfter(right.getSnapshotId(), left.getSnapshotId()) ? right : left;
    }

    /**
     * Check whether text is null or blank.
     *
     * @param value source value
     * @return true when no meaningful text is present
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Parsed physical asset address used to query the event ledger.
     *
     * @param catalogName catalog component
     * @param databaseName database component
     * @param tableName table component
     * @param partitionName optional partition suffix
     */
    private record AssetAddress(
            String catalogName,
            String databaseName,
            String tableName,
            String partitionName) {
    }
}
