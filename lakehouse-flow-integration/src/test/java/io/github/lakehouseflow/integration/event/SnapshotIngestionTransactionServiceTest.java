package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EventConsumerOffset;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.service.AssetStateService;
import io.github.lakehouseflow.service.DagProgressionService;
import io.github.lakehouseflow.service.FlowPlanEvaluationService;
import io.github.lakehouseflow.service.SnapshotTriggerRoutingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the atomic projection steps applied to one lakehouse snapshot.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotIngestionTransactionServiceTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 10, 0);
    private static final Comparator<String> NUMERIC_OFFSET_COMPARATOR =
            Comparator.comparingLong(Long::parseLong);

    @Mock
    private LakehouseEventRepository lakehouseEventRepository;

    @Mock
    private EventConsumerOffsetRepository eventConsumerOffsetRepository;

    @Mock
    private AssetStateService assetStateService;

    @Mock
    private DagProgressionService dagProgressionService;

    @Mock
    private FlowPlanEvaluationService flowPlanEvaluationService;

    @Mock
    private SnapshotTriggerRoutingService snapshotTriggerRoutingService;

    @InjectMocks
    private SnapshotIngestionTransactionService transactionService;

    /**
     * Verify a new current event projects state, evaluates FlowPlans, and saves offset.
     */
    @Test
    void processSnapshotProjectsNewEventAndAdvancesOffset() {
        LakehouseEvent event = event("1000");
        when(lakehouseEventRepository.findByEventId("event-1000")).thenReturn(Optional.empty());
        when(lakehouseEventRepository.save(event)).thenReturn(event);
        when(assetStateService.projectAssetStatesFromEvent(event)).thenReturn(List.of(state("1000")));
        when(snapshotTriggerRoutingService.classify(event)).thenReturn(
                new SnapshotTriggerRoutingService.SnapshotTriggerRoute(
                        true,
                        "EXTERNAL",
                        null,
                        null,
                        "external data"));
        when(eventConsumerOffsetRepository.findForUpdate("PAIMON", "catalog.db.orders"))
                .thenReturn(Optional.empty());

        SnapshotIngestionTransactionService.SnapshotIngestionResult result =
                transactionService.processSnapshot(
                        "catalog.db.orders", "1000", NUMERIC_OFFSET_COMPARATOR, event);

        assertTrue(result.inserted());
        verify(dagProgressionService).onAssetStateAdvanced("catalog.db.orders", BIZ_DATE);
        verify(flowPlanEvaluationService).evaluateTriggeredAssets(
                List.of("catalog.db.orders"),
                "1000",
                BIZ_DATE);
        ArgumentCaptor<EventConsumerOffset> offset = ArgumentCaptor.forClass(EventConsumerOffset.class);
        verify(eventConsumerOffsetRepository).save(offset.capture());
        assertEquals("1000", offset.getValue().getOffsetValue());
    }

    /**
     * Verify a duplicate stale event cannot regress state evaluation or source offset.
     */
    @Test
    void processSnapshotIgnoresStaleDuplicateAndOlderOffset() {
        LakehouseEvent event = event("1000");
        EventConsumerOffset offset = EventConsumerOffset.builder().offsetValue("1001").build();
        when(lakehouseEventRepository.findByEventId("event-1000")).thenReturn(Optional.of(event));
        when(assetStateService.projectAssetStatesFromEvent(event)).thenReturn(List.of(state("1001")));
        when(eventConsumerOffsetRepository.findForUpdate("PAIMON", "catalog.db.orders"))
                .thenReturn(Optional.of(offset));

        SnapshotIngestionTransactionService.SnapshotIngestionResult result =
                transactionService.processSnapshot(
                        "catalog.db.orders", "1000", NUMERIC_OFFSET_COMPARATOR, event);

        assertFalse(result.inserted());
        assertEquals("1001", offset.getOffsetValue());
        verify(lakehouseEventRepository, never()).save(any(LakehouseEvent.class));
        verify(dagProgressionService, never()).onAssetStateAdvanced(any(), any());
        verify(flowPlanEvaluationService, never()).evaluateTriggeredAssets(any(), any(), any());
        verify(snapshotTriggerRoutingService, never()).classify(any());
        verify(eventConsumerOffsetRepository, never()).save(offset);
    }

    /**
     * Verify an action-owned snapshot updates facts but cannot start a normal flow.
     */
    @Test
    void processSnapshotSuppressesNaturalProgressionForActionOwnedSnapshot() {
        LakehouseEvent event = event("1000");
        when(lakehouseEventRepository.findByEventId("event-1000")).thenReturn(Optional.empty());
        when(lakehouseEventRepository.save(event)).thenReturn(event);
        when(assetStateService.projectAssetStatesFromEvent(event)).thenReturn(List.of(state("1000")));
        when(snapshotTriggerRoutingService.classify(event)).thenReturn(
                new SnapshotTriggerRoutingService.SnapshotTriggerRoute(
                        false,
                        "LAKEHOUSE_FLOW_ACTION",
                        "task-instance:42",
                        BIZ_DATE,
                        "action owned"));
        when(eventConsumerOffsetRepository.findForUpdate("PAIMON", "catalog.db.orders"))
                .thenReturn(Optional.empty());

        SnapshotIngestionTransactionService.SnapshotIngestionResult result =
                transactionService.processSnapshot(
                        "catalog.db.orders", "1000", NUMERIC_OFFSET_COMPARATOR, event);

        assertTrue(result.inserted());
        verify(assetStateService).projectAssetStatesFromEvent(event);
        verify(dagProgressionService, never()).onAssetStateAdvanced(any(), any());
        verify(flowPlanEvaluationService, never()).evaluateTriggeredAssets(any(), any(), any());
    }

    /**
     * Verify a natural intent uses its frozen date across table and partition projections.
     */
    @Test
    void processSnapshotUsesIntentDateForNaturalPartitionProgression() {
        LakehouseEvent event = event("1000");
        LocalDateTime intentDate = LocalDateTime.of(2026, 9, 1, 0, 0);
        AssetState tableState = state("1000");
        AssetState partitionState = AssetState.builder()
                .assetKey("catalog.db.orders.dt=2026-09-01")
                .latestSnapshotId("1000")
                .build();
        when(lakehouseEventRepository.findByEventId("event-1000")).thenReturn(Optional.empty());
        when(lakehouseEventRepository.save(event)).thenReturn(event);
        when(assetStateService.projectAssetStatesFromEvent(event))
                .thenReturn(List.of(tableState, partitionState));
        when(snapshotTriggerRoutingService.classify(event)).thenReturn(
                new SnapshotTriggerRoutingService.SnapshotTriggerRoute(
                        true,
                        "LAKEHOUSE_FLOW_NATURAL",
                        "task-instance:42",
                        intentDate,
                        "natural intent"));
        when(eventConsumerOffsetRepository.findForUpdate("PAIMON", "catalog.db.orders"))
                .thenReturn(Optional.empty());

        transactionService.processSnapshot(
                "catalog.db.orders", "1000", NUMERIC_OFFSET_COMPARATOR, event);

        verify(dagProgressionService).onAssetStateAdvanced("catalog.db.orders", intentDate);
        verify(dagProgressionService).onAssetStateAdvanced(
                "catalog.db.orders.dt=2026-09-01",
                intentDate);
        verify(flowPlanEvaluationService).evaluateTriggeredAssets(
                List.of("catalog.db.orders", "catalog.db.orders.dt=2026-09-01"),
                "1000",
                intentDate);
    }

    /**
     * Verify offset ownership follows the candidate format instead of a Paimon constant.
     */
    @Test
    void processSnapshotUsesCandidateSourceTypeForOffset() {
        LakehouseEvent event = event("iceberg-snapshot-7");
        event.setSourceType("ICEBERG");
        when(lakehouseEventRepository.findByEventId("event-iceberg-snapshot-7"))
                .thenReturn(Optional.empty());
        when(lakehouseEventRepository.save(event)).thenReturn(event);
        when(assetStateService.projectAssetStatesFromEvent(event)).thenReturn(List.of());
        when(eventConsumerOffsetRepository.findForUpdate(
                "ICEBERG", "catalog.db.orders")).thenReturn(Optional.empty());

        transactionService.processSnapshot(
                "catalog.db.orders", "snapshot-7", Comparator.naturalOrder(), event);

        ArgumentCaptor<EventConsumerOffset> offset = ArgumentCaptor.forClass(EventConsumerOffset.class);
        verify(eventConsumerOffsetRepository).save(offset.capture());
        assertEquals("ICEBERG", offset.getValue().getSourceType());
        assertEquals("snapshot-7", offset.getValue().getOffsetValue());
    }

    /**
     * Verify an adapter's opaque offset ordering controls durable advancement.
     */
    @Test
    void processSnapshotUsesFormatSpecificOffsetComparator() {
        LakehouseEvent event = event("iceberg-sequence-10");
        event.setSourceType("ICEBERG");
        EventConsumerOffset offset = EventConsumerOffset.builder()
                .offsetValue("sequence-2")
                .build();
        Comparator<String> sequenceComparator = Comparator.comparingInt(
                value -> Integer.parseInt(value.substring("sequence-".length())));
        when(lakehouseEventRepository.findByEventId("event-iceberg-sequence-10"))
                .thenReturn(Optional.of(event));
        when(assetStateService.projectAssetStatesFromEvent(event)).thenReturn(List.of());
        when(eventConsumerOffsetRepository.findForUpdate(
                "ICEBERG", "catalog.db.orders")).thenReturn(Optional.of(offset));
        when(eventConsumerOffsetRepository.save(offset)).thenReturn(offset);

        transactionService.processSnapshot(
                "catalog.db.orders", "sequence-10", sequenceComparator, event);

        assertEquals("sequence-10", offset.getOffsetValue());
        verify(eventConsumerOffsetRepository).save(offset);
    }

    /**
     * Build one source event fixture.
     *
     * @param snapshotId source snapshot id
     * @return lakehouse event
     */
    private LakehouseEvent event(String snapshotId) {
        return LakehouseEvent.builder()
                .eventId("event-" + snapshotId)
                .eventType("SNAPSHOT_COMMITTED")
                .sourceType("PAIMON")
                .catalogName("catalog")
                .databaseName("db")
                .tableName("orders")
                .snapshotId(snapshotId)
                .commitKind("APPEND")
                .watermark(BIZ_DATE)
                .build();
    }

    /**
     * Build projected AssetState evidence.
     *
     * @param snapshotId latest snapshot id
     * @return asset state
     */
    private AssetState state(String snapshotId) {
        return AssetState.builder()
                .assetKey("catalog.db.orders")
                .latestSnapshotId(snapshotId)
                .build();
    }
}
