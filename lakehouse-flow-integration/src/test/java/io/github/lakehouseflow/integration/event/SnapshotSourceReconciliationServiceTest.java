package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSource;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSourceRegistry;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;
import io.github.lakehouseflow.integration.source.SnapshotSourcePosition;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EventConsumerOffset;
import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests source, offset, event, and AssetState reconciliation with bounded compensation.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotSourceReconciliationServiceTest {

    private static final LakehouseSourceIdentity IDENTITY =
            new LakehouseSourceIdentity("PAIMON", "orders", "lake", "ods", "orders");

    @Mock
    private LakehouseSnapshotSourceRegistry sourceRegistry;

    @Mock
    private EventConsumerOffsetRepository offsetRepository;

    @Mock
    private LakehouseEventRepository eventRepository;

    @Mock
    private AssetStateRepository assetStateRepository;

    @Mock
    private EventIngestionService ingestionService;

    @Mock
    private SnapshotIngestionTransactionService transactionService;

    @Mock
    private SnapshotSourceMetrics metrics;

    @Mock
    private LakehouseSnapshotSource source;

    private SnapshotSourceReconciliationService reconciliationService;

    /** Create the service and one stable source identity. */
    @BeforeEach
    void setUp() {
        reconciliationService = new SnapshotSourceReconciliationService(
                sourceRegistry,
                offsetRepository,
                eventRepository,
                assetStateRepository,
                ingestionService,
                transactionService,
                metrics);
        when(source.identity()).thenReturn(IDENTITY);
    }

    /** Verify a current offset, event, and AssetState form healthy snapshot evidence. */
    @Test
    void reconcileSourceReportsHealthyEvidence() {
        stubInspection("10", inSync("10"), event("10"), state("10"));

        SnapshotSourceReconciliation result = reconciliationService.reconcileSource(source);

        assertEquals(SnapshotSourceReconciliationOutcome.HEALTHY, result.outcome());
        assertEquals(SnapshotProjectionStatus.CONSISTENT, result.projectionStatus());
        assertEquals("10", result.latestDataEventSnapshotId());
        assertEquals("10", result.assetStateDataSnapshotId());
        verify(metrics).recordReconciliation(result);
    }

    /** Verify all-source inspection blocks a retention gap without advancing the offset. */
    @Test
    void reconcileAllSourcesBlocksRetentionGap() {
        when(sourceRegistry.sources()).thenReturn(List.of(source));
        SnapshotSourcePosition gap = new SnapshotSourcePosition(
                SnapshotSourceOffsetStatus.RETENTION_GAP,
                "8", "10", "12", "12", 3L, "retention gap");
        stubInspection("8", gap, event("8"), state("8"));

        List<SnapshotSourceReconciliation> results = reconciliationService.reconcileAllSources();

        assertEquals(SnapshotSourceReconciliationOutcome.BLOCKED, results.get(0).outcome());
        verify(ingestionService, never()).ingestSource(any());
        verifyNoInteractions(transactionService);
    }

    /** Verify a lagging source is compensated by one bounded normal ingestion pass. */
    @Test
    void reconcileAndRepairAllSourcesIngestsLaggingOffsets() {
        when(sourceRegistry.sources()).thenReturn(List.of(source));
        EventConsumerOffset offset9 = EventConsumerOffset.builder().offsetValue("9").build();
        EventConsumerOffset offset10 = EventConsumerOffset.builder().offsetValue("10").build();
        when(offsetRepository.findBySourceTypeAndSourceName("PAIMON", "orders"))
                .thenReturn(Optional.of(offset9), Optional.of(offset10));
        when(source.inspectPosition("9")).thenReturn(new SnapshotSourcePosition(
                SnapshotSourceOffsetStatus.LAGGING,
                "9", "1", "10", "10", 1L, "lagging"));
        when(source.inspectPosition("10")).thenReturn(inSync("10"));
        when(eventRepository.findLatestSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(event("9")), Optional.of(event("10")));
        when(eventRepository.findLatestDataSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(event("9")), Optional.of(event("10")));
        when(assetStateRepository.findByAssetKey("lake.ods.orders"))
                .thenReturn(Optional.of(state("9")), Optional.of(state("10")));
        when(ingestionService.ingestSource(source)).thenReturn(1);

        SnapshotSourceReconciliation result =
                reconciliationService.reconcileAndRepairAllSources().get(0);

        assertEquals(SnapshotSourceReconciliationOutcome.HEALTHY, result.outcome());
        assertTrue(result.repairAttempted());
        assertEquals(1, result.repairedEventCount());
        verify(ingestionService).ingestSource(source);
    }

    /** Verify a lagging offset without its durable event remains blocked and cannot advance. */
    @Test
    void reconcileAndRepairAllSourcesDoesNotAdvanceBlockedLag() {
        when(sourceRegistry.sources()).thenReturn(List.of(source));
        when(offsetRepository.findBySourceTypeAndSourceName("PAIMON", "orders"))
                .thenReturn(Optional.of(EventConsumerOffset.builder().offsetValue("9").build()));
        when(source.inspectPosition("9")).thenReturn(new SnapshotSourcePosition(
                SnapshotSourceOffsetStatus.LAGGING,
                "9", "1", "10", "10", 1L, "lagging"));
        when(eventRepository.findLatestSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.empty());
        when(assetStateRepository.findByAssetKey("lake.ods.orders"))
                .thenReturn(Optional.of(state("9")));

        SnapshotSourceReconciliation result =
                reconciliationService.reconcileAndRepairAllSources().get(0);

        assertEquals(SnapshotSourceReconciliationOutcome.BLOCKED, result.outcome());
        assertEquals(SnapshotProjectionStatus.MISSING_EVENT, result.projectionStatus());
        verify(ingestionService, never()).ingestSource(any());
        verifyNoInteractions(transactionService);
    }

    /** Verify an existing durable event can rebuild a missing or stale AssetState projection. */
    @Test
    void reconcileAndRepairAllSourcesReplaysProjectionDrift() {
        when(sourceRegistry.sources()).thenReturn(List.of(source));
        EventConsumerOffset offset = EventConsumerOffset.builder().offsetValue("10").build();
        LakehouseEvent event = event("10");
        when(offsetRepository.findBySourceTypeAndSourceName("PAIMON", "orders"))
                .thenReturn(Optional.of(offset));
        when(source.inspectPosition("10")).thenReturn(inSync("10"));
        when(source.offsetComparator()).thenReturn(Comparator.comparingLong(Long::parseLong));
        when(eventRepository.findLatestSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(event));
        when(eventRepository.findLatestDataSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(event));
        when(assetStateRepository.findByAssetKey("lake.ods.orders"))
                .thenReturn(Optional.of(state("9")), Optional.of(state("10")));

        SnapshotSourceReconciliation result =
                reconciliationService.reconcileAndRepairAllSources().get(0);

        assertEquals(SnapshotSourceReconciliationOutcome.HEALTHY, result.outcome());
        assertTrue(result.repairAttempted());
        verify(transactionService).processSnapshot(
                "orders",
                "10",
                source.offsetComparator(),
                event);
    }

    /** Verify reconciliation replays the latest data event behind a newer compaction. */
    @Test
    void reconcileAndRepairAllSourcesReplaysDataProjectionBehindCompaction() {
        when(sourceRegistry.sources()).thenReturn(List.of(source));
        EventConsumerOffset offset = EventConsumerOffset.builder().offsetValue("11").build();
        LakehouseEvent compact = event("11", false);
        LakehouseEvent data = event("10", true);
        when(offsetRepository.findBySourceTypeAndSourceName("PAIMON", "orders"))
                .thenReturn(Optional.of(offset));
        when(source.inspectPosition("11")).thenReturn(inSync("11"));
        when(source.offsetComparator()).thenReturn(Comparator.comparingLong(Long::parseLong));
        when(eventRepository.findLatestSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(compact));
        when(eventRepository.findLatestDataSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(data));
        when(assetStateRepository.findByAssetKey("lake.ods.orders"))
                .thenReturn(Optional.of(state("11", "9")), Optional.of(state("11", "10")));

        SnapshotSourceReconciliation result =
                reconciliationService.reconcileAndRepairAllSources().get(0);

        assertEquals(SnapshotSourceReconciliationOutcome.HEALTHY, result.outcome());
        assertTrue(result.repairAttempted());
        verify(transactionService).processSnapshot(
                "orders",
                "11",
                source.offsetComparator(),
                data);
    }

    /** Stub one complete reconciliation inspection. */
    private void stubInspection(
            String durableOffset,
            SnapshotSourcePosition position,
            LakehouseEvent event,
            AssetState state) {
        when(offsetRepository.findBySourceTypeAndSourceName("PAIMON", "orders"))
                .thenReturn(Optional.of(EventConsumerOffset.builder().offsetValue(durableOffset).build()));
        when(source.inspectPosition(durableOffset)).thenReturn(position);
        when(eventRepository.findLatestSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(event));
        when(eventRepository.findLatestDataSourceEvent("PAIMON", "lake", "ods", "orders"))
                .thenReturn(Optional.of(event));
        when(assetStateRepository.findByAssetKey("lake.ods.orders")).thenReturn(Optional.of(state));
    }

    /** Build an in-sync source position. */
    private SnapshotSourcePosition inSync(String offset) {
        return new SnapshotSourcePosition(
                SnapshotSourceOffsetStatus.IN_SYNC,
                offset, "1", offset, offset, 0L, "in sync");
    }

    /** Build one durable event fixture. */
    private LakehouseEvent event(String snapshotId) {
        return event(snapshotId, true);
    }

    /** Build one durable event fixture with an explicit data classification. */
    private LakehouseEvent event(String snapshotId, boolean dataChange) {
        return LakehouseEvent.builder()
                .sourceType("PAIMON")
                .catalogName("lake")
                .databaseName("ods")
                .tableName("orders")
                .snapshotId(snapshotId)
                .dataChange(dataChange)
                .build();
    }

    /** Build one table-level AssetState fixture. */
    private AssetState state(String snapshotId) {
        return state(snapshotId, snapshotId);
    }

    /** Build one table-level AssetState fixture with split snapshot coordinates. */
    private AssetState state(String snapshotId, String dataSnapshotId) {
        return AssetState.builder()
                .assetKey("lake.ods.orders")
                .latestSnapshotId(snapshotId)
                .latestDataSnapshotId(dataSnapshotId)
                .build();
    }
}
