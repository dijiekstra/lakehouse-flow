package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.WriterJobBindingRepository;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.WriterJobBinding;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Routes snapshot facts between natural triggering and action-owned progress.
 *
 * Backfill and rerun snapshots remain visible in the event ledger and
 * AssetState, but they must only confirm their immutable scheduling intent.
 * They cannot create an additional snapshot-driven workflow instance.
 */
@Service
@RequiredArgsConstructor
public class SnapshotTriggerRoutingService {

    private static final Set<String> ACTION_OWNED_TRIGGER_TYPES = Set.of(
            "BACKFILL",
            "BACKFILL_RECOVERY",
            "RERUN",
            "RERUN_TASK",
            "MANUAL_ACTION");

    private final SchedulingIntentRepository schedulingIntentRepository;
    private final JobControlIntentRepository jobControlIntentRepository;
    private final WriterJobBindingRepository writerJobBindingRepository;

    /**
     * Classify one durable snapshot before natural DAG and FlowPlan evaluation.
     *
     * External data commits are eligible for natural triggering. A snapshot
     * claiming Lakehouse Flow ownership is eligible only when its final
     * attribution properties match an existing non-action intent exactly.
     *
     * @param event durable snapshot observation
     * @return immutable routing decision and audit reason
     */
    @Transactional(readOnly = true)
    public SnapshotTriggerRoute classify(LakehouseEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("snapshot event is required");
        }

        Map<?, ?> properties = snapshotProperties(event);
        String source = property(properties, SnapshotEvidenceContract.SOURCE_PROPERTY).orElse(null);
        if (!SnapshotEvidenceContract.INTENT_SOURCE.equals(source)) {
            return isAcceptedDataCommit(event)
                    ? SnapshotTriggerRoute.natural("EXTERNAL", null, null,
                            "External data snapshot may drive natural scheduling")
                    : SnapshotTriggerRoute.suppressed("MAINTENANCE", null,
                            "Non-data snapshot cannot drive natural scheduling");
        }

        String intentKey = property(properties, SnapshotEvidenceContract.INTENT_KEY_PROPERTY).orElse(null);
        if (isBlank(intentKey)) {
            return classifyJobControlSnapshot(event, properties);
        }

        Optional<SchedulingIntent> intent = schedulingIntentRepository.findByIntentKey(intentKey);
        if (intent.isEmpty()) {
            return SnapshotTriggerRoute.suppressed("LAKEHOUSE_FLOW_ORPHAN", intentKey,
                    "Snapshot intent key does not belong to a persisted scheduling intent");
        }
        SchedulingIntent matchedIntent = intent.get();
        if (!matchesFinalIntentSnapshot(event, properties, matchedIntent)) {
            return SnapshotTriggerRoute.suppressed("LAKEHOUSE_FLOW_INTERMEDIATE", intentKey,
                    "Snapshot is not the valid final output of its scheduling intent");
        }
        if (isActionOwned(matchedIntent.getTriggerType())) {
            return SnapshotTriggerRoute.actionOwned(
                    intentKey,
                    matchedIntent.getBizDate(),
                    "Action-owned snapshot only advances its existing scheduling instance");
        }
        return SnapshotTriggerRoute.natural(
                "LAKEHOUSE_FLOW_NATURAL",
                intentKey,
                matchedIntent.getBizDate(),
                "Final snapshot from a natural scheduling intent may drive downstream plans");
    }

    /** Classify a snapshot carrying only writer-generation control attribution. */
    private SnapshotTriggerRoute classifyJobControlSnapshot(LakehouseEvent event, Map<?, ?> properties) {
        String controlIntentKey = property(
                properties,
                SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY).orElse(null);
        if (isBlank(controlIntentKey)) {
            return SnapshotTriggerRoute.suppressed("LAKEHOUSE_FLOW_UNATTRIBUTED", null,
                    "Lakehouse Flow source marker has no data or job-control intent key");
        }
        Optional<JobControlIntent> intent = jobControlIntentRepository.findByIntentKey(controlIntentKey);
        if (intent.isEmpty()) {
            return SnapshotTriggerRoute.suppressed("LAKEHOUSE_FLOW_JOB_CONTROL_ORPHAN", controlIntentKey,
                    "Writer snapshot control key does not belong to a persisted intent");
        }
        JobControlIntent matchedIntent = intent.get();
        if (!matchesJobControlSnapshot(event, properties, matchedIntent)
                || !isCurrentWriterGeneration(
                        matchedIntent.getWriterJobKey(),
                        matchedIntent.getWriterEpoch(),
                        matchedIntent.getIntentKey())) {
            return SnapshotTriggerRoute.suppressed("LAKEHOUSE_FLOW_STALE_WRITER", controlIntentKey,
                    "Writer snapshot does not match the currently authorized generation");
        }
        return SnapshotTriggerRoute.natural(
                "LAKEHOUSE_FLOW_JOB_CONTROL",
                controlIntentKey,
                null,
                "Current platform-controlled writer data may drive natural scheduling");
    }

    /**
     * Check the complete final-snapshot attribution contract for routing.
     *
     * @param event observed snapshot event
     * @param properties observed snapshot properties
     * @param intent persisted scheduling intent
     * @return true when the event is the intent's valid final data snapshot
     */
    private boolean matchesFinalIntentSnapshot(
            LakehouseEvent event,
            Map<?, ?> properties,
            SchedulingIntent intent) {
        return isAcceptedDataCommit(event)
                && matchesTargetTable(event, intent.getTargetAssetKey())
                && isCurrentWriterGeneration(intent.getWriterJobKey(), intent.getWriterEpoch(), null)
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY,
                        intent.getWriterJobKey())
                && intent.getWriterEpoch() != null
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY,
                        Long.toString(intent.getWriterEpoch()))
                && propertyEquals(properties,
                        SnapshotEvidenceContract.TARGET_ASSET_PROPERTY,
                        intent.getTargetAssetKey())
                && intent.getBizDate() != null
                && propertyEquals(properties,
                        SnapshotEvidenceContract.BIZ_DATE_PROPERTY,
                        intent.getBizDate().toLocalDate().toString())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.FINAL_PROPERTY,
                        SnapshotEvidenceContract.FINAL_VALUE)
                && changesExpectedPartition(event, extractPartition(intent.getTargetAssetKey()));
    }

    /** Match a current control intent to one target-table business snapshot. */
    private boolean matchesJobControlSnapshot(
            LakehouseEvent event,
            Map<?, ?> properties,
            JobControlIntent intent) {
        return isAcceptedDataCommit(event)
                && matchesTargetTable(event, intent.getTableAssetKey())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY,
                        intent.getIntentKey())
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY,
                        intent.getWriterJobKey())
                && intent.getWriterEpoch() != null
                && propertyEquals(properties,
                        SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY,
                        Long.toString(intent.getWriterEpoch()));
    }

    /** Verify the event writer epoch has not been superseded by a newer generation. */
    private boolean isCurrentWriterGeneration(
            String writerJobKey,
            Long writerEpoch,
            String requiredControlIntentKey) {
        if (isBlank(writerJobKey) || writerEpoch == null || writerEpoch <= 0) {
            return false;
        }
        Optional<WriterJobBinding> binding = writerJobBindingRepository.findByWriterJobKey(writerJobKey);
        if (binding.isEmpty() || !writerEpoch.equals(binding.get().getCurrentWriterEpoch())) {
            return false;
        }
        return requiredControlIntentKey == null
                || requiredControlIntentKey.equals(binding.get().getCurrentControlIntentKey());
    }

    /**
     * Verify that the event came from the physical table named by the intent.
     *
     * @param event observed snapshot event
     * @param targetAssetKey expected catalog.database.table[.partition] key
     * @return true when catalog, database, and table match exactly
     */
    private boolean matchesTargetTable(LakehouseEvent event, String targetAssetKey) {
        if (isBlank(targetAssetKey)) {
            return false;
        }
        String[] parts = targetAssetKey.split("\\.", 4);
        return parts.length >= 3
                && parts[0].equals(event.getCatalogName())
                && parts[1].equals(event.getDatabaseName())
                && parts[2].equals(event.getTableName());
    }

    /**
     * Check whether a trigger belongs to an explicit replay or repair action.
     *
     * @param triggerType persisted workflow trigger type
     * @return true when natural triggering must be suppressed
     */
    private boolean isActionOwned(String triggerType) {
        return triggerType != null
                && ACTION_OWNED_TRIGGER_TYPES.contains(triggerType.trim().toUpperCase(Locale.ROOT));
    }

    /**
     * Accept only commits that can represent produced business data.
     *
     * @param event observed snapshot event
     * @return true when its source adapter classified it as a business-data change
     */
    private boolean isAcceptedDataCommit(LakehouseEvent event) {
        return SnapshotEventChangeClassifier.isDataChange(event);
    }

    /**
     * Verify partition evidence for a partition-scoped intent.
     *
     * @param event observed snapshot event
     * @param expectedPartition expected partition suffix, or null
     * @return true when table scope applies or the expected partition changed
     */
    private boolean changesExpectedPartition(LakehouseEvent event, String expectedPartition) {
        if (isBlank(expectedPartition)) {
            return true;
        }
        if (expectedPartition.equals(event.getPartitionName())) {
            return true;
        }
        Object rawPartitions = event.getPayloadJson() == null
                ? null
                : event.getPayloadJson().get("changedPartitions");
        return rawPartitions instanceof Collection<?> partitions
                && partitions.stream().anyMatch(expectedPartition::equals);
    }

    /**
     * Extract the optional partition suffix from a physical asset key.
     *
     * @param assetKey catalog.database.table[.partition] asset key
     * @return partition suffix, or null for table scope
     */
    private String extractPartition(String assetKey) {
        if (isBlank(assetKey)) {
            return null;
        }
        String[] parts = assetKey.split("\\.", 4);
        return parts.length == 4 ? parts[3] : null;
    }

    /**
     * Read snapshot properties from the raw event payload.
     *
     * @param event observed snapshot event
     * @return property map, or an empty map when absent
     */
    private Map<?, ?> snapshotProperties(LakehouseEvent event) {
        if (event.getPayloadJson() == null) {
            return Map.of();
        }
        Object rawProperties = event.getPayloadJson().get("snapshotProperties");
        return rawProperties instanceof Map<?, ?> properties ? properties : Map.of();
    }

    /**
     * Read one snapshot property as text.
     *
     * @param properties observed snapshot properties
     * @param key property key
     * @return textual property value when present
     */
    private Optional<String> property(Map<?, ?> properties, String key) {
        return Optional.ofNullable(properties.get(key)).map(String::valueOf);
    }

    /**
     * Compare one observed property with its required value.
     *
     * @param properties observed snapshot properties
     * @param key property key
     * @param expected required value
     * @return true when values match exactly
     */
    private boolean propertyEquals(Map<?, ?> properties, String key, String expected) {
        return expected != null && expected.equals(property(properties, key).orElse(null));
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
     * Immutable decision controlling whether a snapshot enters natural flow evaluation.
     *
     * @param naturalProgressionAllowed whether global DAG and FlowPlan evaluation may run
     * @param originType classified snapshot origin
     * @param intentKey matched scheduling intent key, or null
     * @param intentBizDate business date frozen by the matched intent, or null
     * @param reason human-readable routing reason
     */
    public record SnapshotTriggerRoute(
            boolean naturalProgressionAllowed,
            String originType,
            String intentKey,
            LocalDateTime intentBizDate,
            String reason) {

        /**
         * Build a natural-progression route.
         *
         * @param originType classified origin
         * @param intentKey matched intent key, or null
         * @param intentBizDate matched intent business date, or null
         * @param reason routing reason
         * @return natural-progression route
         */
        private static SnapshotTriggerRoute natural(
                String originType,
                String intentKey,
                LocalDateTime intentBizDate,
                String reason) {
            return new SnapshotTriggerRoute(true, originType, intentKey, intentBizDate, reason);
        }

        /**
         * Build an action-owned route that suppresses natural triggering.
         *
         * @param intentKey matched intent key
         * @param intentBizDate matched intent business date
         * @param reason routing reason
         * @return action-owned route
         */
        private static SnapshotTriggerRoute actionOwned(
                String intentKey,
                LocalDateTime intentBizDate,
                String reason) {
            return new SnapshotTriggerRoute(false, "LAKEHOUSE_FLOW_ACTION", intentKey, intentBizDate, reason);
        }

        /**
         * Build a fail-closed route for an ineligible snapshot.
         *
         * @param originType classified origin
         * @param intentKey observed intent key, or null
         * @param reason routing reason
         * @return suppressed route
         */
        private static SnapshotTriggerRoute suppressed(String originType, String intentKey, String reason) {
            return new SnapshotTriggerRoute(false, originType, intentKey, null, reason);
        }
    }
}
