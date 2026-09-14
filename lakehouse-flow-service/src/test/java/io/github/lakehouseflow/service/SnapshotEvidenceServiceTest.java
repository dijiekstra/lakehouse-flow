package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.SchedulingIntent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests intent-scoped snapshot attribution over the durable event sequence.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotEvidenceServiceTest {

    private static final String TARGET_ASSET = "paimon.prod.orders.dt=2026-09-13";
    private static final String INTENT_KEY = "task-instance:42";

    @Mock
    private LakehouseEventRepository lakehouseEventRepository;

    @InjectMocks
    private SnapshotEvidenceService snapshotEvidenceService;

    /** Verify an earlier matching snapshot survives a later unrelated write. */
    @Test
    void confirmsMatchingIntentSnapshotInsideInterleavedWrites() {
        SchedulingIntent intent = intent();
        LakehouseEvent externalWrite = event("101", "APPEND", Map.of(), List.of("dt=2026-09-13"));
        LakehouseEvent matchingWrite = event(
                "102",
                "APPEND",
                requiredProperties(),
                List.of("dt=2026-09-13"));
        LakehouseEvent laterExternalWrite = event("103", "OVERWRITE", Map.of(), List.of("dt=2026-09-13"));
        stubEvents(externalWrite, matchingWrite, laterExternalWrite);

        EvaluationResult result = snapshotEvidenceService.evaluateIntentProgress(intent);

        assertTrue(result.getSatisfied());
        assertEquals("102", result.getSnapshotId());
        assertEquals("event-102", result.getEventId());
    }

    /** Verify maintenance snapshots cannot confirm a scheduling intent. */
    @Test
    void ignoresCompactionEvenWhenItCarriesIntentProperties() {
        stubEvents(event(
                "101",
                "COMPACT",
                requiredProperties(),
                List.of("dt=2026-09-13")));

        EvaluationResult result = snapshotEvidenceService.evaluateIntentProgress(intent());

        assertFalse(result.getSatisfied());
        assertEquals("101", result.getSnapshotId());
        assertTrue(result.getWaitingReason().contains("none matched"));
    }

    /** Verify source adapters can classify non-Paimon commit kinds as data changes. */
    @Test
    void confirmsFormatNeutralDataChange() {
        LakehouseEvent hudiCommit = event(
                "101",
                "DELTA_COMMIT",
                requiredProperties(),
                List.of("dt=2026-09-13"));
        hudiCommit.getPayloadJson().put(SnapshotEvidenceContract.DATA_CHANGE_FIELD, true);
        stubEvents(hudiCommit);

        EvaluationResult result = snapshotEvidenceService.evaluateIntentProgress(intent());

        assertTrue(result.getSatisfied());
        assertEquals("101", result.getSnapshotId());
    }

    /** Verify exact intent markers still require the expected target partition. */
    @Test
    void rejectsMatchingMarkerForDifferentPartition() {
        stubEvents(event(
                "101",
                "APPEND",
                requiredProperties(),
                List.of("dt=2026-09-12")));

        EvaluationResult result = snapshotEvidenceService.evaluateIntentProgress(intent());

        assertFalse(result.getSatisfied());
        assertTrue(result.getWaitingReason().contains(INTENT_KEY));
    }

    /** Verify malformed target keys fail before an ambiguous event query. */
    @Test
    void rejectsMalformedTargetAssetKey() {
        SchedulingIntent intent = intent();
        intent.setTargetAssetKey("orders");

        assertThrows(IllegalArgumentException.class,
                () -> snapshotEvidenceService.evaluateIntentProgress(intent));
    }

    /** Verify job-control confirmation requires the exact writer key, epoch, and control key. */
    @Test
    void confirmsJobControlWriterGenerationSnapshot() {
        String controlKey = "job-control:writer.prod.orders:7";
        JobControlIntent intent = JobControlIntent.builder()
                .intentKey(controlKey)
                .writerJobKey("writer.prod.orders")
                .writerEpoch(7L)
                .tableAssetKey("paimon.prod.orders")
                .baselineSnapshotId("100")
                .createdAt(LocalDateTime.of(2026, 9, 13, 1, 0))
                .build();
        stubEvents(event("101", "APPEND", Map.of(
                SnapshotEvidenceContract.SOURCE_PROPERTY, SnapshotEvidenceContract.INTENT_SOURCE,
                SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY, controlKey,
                SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY, "writer.prod.orders",
                SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY, "7"), List.of()));

        EvaluationResult result = snapshotEvidenceService.evaluateJobControlProgress(intent);

        assertTrue(result.getSatisfied());
        assertEquals("101", result.getSnapshotId());
    }

    /** Verify a maintenance snapshot cannot prove a controlled writer generation produced data. */
    @Test
    void jobControlIgnoresMaintenanceSnapshot() {
        JobControlIntent intent = JobControlIntent.builder()
                .intentKey("job-control:writer.prod.orders:7")
                .writerJobKey("writer.prod.orders")
                .writerEpoch(7L)
                .tableAssetKey("paimon.prod.orders")
                .baselineSnapshotId("100")
                .createdAt(LocalDateTime.of(2026, 9, 13, 1, 0))
                .build();
        stubEvents(event("101", "COMPACT", Map.of(
                SnapshotEvidenceContract.SOURCE_PROPERTY, SnapshotEvidenceContract.INTENT_SOURCE,
                SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY, intent.getIntentKey(),
                SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY, "writer.prod.orders",
                SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY, "7"), List.of()));

        EvaluationResult result = snapshotEvidenceService.evaluateJobControlProgress(intent);

        assertFalse(result.getSatisfied());
    }

    /** Stub one ordered physical-table event sequence. */
    private void stubEvents(LakehouseEvent... events) {
        when(lakehouseEventRepository
                .findSnapshotEvidenceCandidates(
                        "paimon",
                        "prod",
                        "orders",
                        LocalDateTime.of(2026, 9, 13, 1, 0)))
                .thenReturn(List.of(events));
    }

    /** Build the immutable intent being confirmed. */
    private SchedulingIntent intent() {
        return SchedulingIntent.builder()
                .intentKey(INTENT_KEY)
                .bizDate(LocalDateTime.of(2026, 9, 13, 0, 0))
                .targetAssetKey(TARGET_ASSET)
                .baselineSnapshotId("100")
                .writerJobKey("writer.prod.orders")
                .writerEpoch(7L)
                .createdAt(LocalDateTime.of(2026, 9, 13, 1, 0))
                .build();
    }

    /** Build one raw snapshot event with attribution and partition metadata. */
    private LakehouseEvent event(
            String snapshotId,
            String commitKind,
            Map<String, String> properties,
            List<String> changedPartitions) {
        return LakehouseEvent.builder()
                .eventId("event-" + snapshotId)
                .snapshotId(snapshotId)
                .commitKind(commitKind)
                .payloadJson(new java.util.LinkedHashMap<>(Map.of(
                        "snapshotProperties", properties,
                        "changedPartitions", changedPartitions,
                        SnapshotEvidenceContract.DATA_CHANGE_FIELD,
                        "APPEND".equals(commitKind) || "OVERWRITE".equals(commitKind))))
                .build();
    }

    /** Build the exact snapshot properties required by the scheduling intent. */
    private Map<String, String> requiredProperties() {
        return Map.of(
                SnapshotEvidenceContract.SOURCE_PROPERTY, SnapshotEvidenceContract.INTENT_SOURCE,
                SnapshotEvidenceContract.INTENT_KEY_PROPERTY, INTENT_KEY,
                SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY, "writer.prod.orders",
                SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY, "7",
                SnapshotEvidenceContract.TARGET_ASSET_PROPERTY, TARGET_ASSET,
                SnapshotEvidenceContract.BIZ_DATE_PROPERTY, "2026-09-13",
                SnapshotEvidenceContract.FINAL_PROPERTY, SnapshotEvidenceContract.FINAL_VALUE);
    }
}
