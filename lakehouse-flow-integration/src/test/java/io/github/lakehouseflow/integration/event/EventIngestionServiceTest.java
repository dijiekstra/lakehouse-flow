package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.integration.source.LakehouseSnapshot;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotEventMapper;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSource;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSourceProvider;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSourceRegistry;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.model.EventConsumerOffset;
import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests format-neutral source discovery and ordered transactional ingestion.
 */
@ExtendWith(MockitoExtension.class)
class EventIngestionServiceTest {

    private static final LakehouseSourceIdentity IDENTITY = new LakehouseSourceIdentity(
            "ICEBERG", "catalog.db.orders", "catalog", "db", "orders");
    private static final Comparator<String> NUMERIC_OFFSET_COMPARATOR =
            Comparator.comparingLong(Long::parseLong);

    @Mock
    private LakehouseSnapshotSourceProvider sourceProvider;

    @Mock
    private LakehouseSnapshotSource source;

    @Mock
    private EventConsumerOffsetRepository eventConsumerOffsetRepository;

    @Mock
    private LakehouseSnapshotEventMapper eventMapper;

    @Mock
    private SnapshotIngestionTransactionService transactionService;

    @Mock
    private SnapshotSourceMetrics snapshotSourceMetrics;

    private EventIngestionService ingestionService;

    /**
     * Create the service with one mockable format provider.
     */
    @BeforeEach
    void setUp() {
        ingestionService = new EventIngestionService(
                new LakehouseSnapshotSourceRegistry(List.of(sourceProvider)),
                eventConsumerOffsetRepository,
                eventMapper,
                transactionService,
                snapshotSourceMetrics);
    }

    /**
     * Verify all configured format sources are discovered and ingested.
     */
    @Test
    void ingestAllSourcesDiscoversProviderSources() {
        stubIdentity();
        when(sourceProvider.sources()).thenReturn(List.of(source));
        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "ICEBERG", "catalog.db.orders")).thenReturn(Optional.empty());
        when(source.scanAfter(null)).thenReturn(List.of());

        assertEquals(0, ingestionService.ingestAllSources());

        verify(source).scanAfter(null);
        verify(snapshotSourceMetrics).recordScan(
                org.mockito.ArgumentMatchers.eq(IDENTITY),
                org.mockito.ArgumentMatchers.eq("success"),
                org.mockito.ArgumentMatchers.eq(0),
                any(java.time.Duration.class));
    }

    /**
     * Verify source offsets are sorted with the adapter's own comparator.
     */
    @Test
    void ingestSourceProcessesSnapshotsInFormatOrder() {
        stubIdentity();
        when(source.offsetComparator()).thenReturn(NUMERIC_OFFSET_COMPARATOR);
        LakehouseSnapshot newer = snapshot("11");
        LakehouseSnapshot older = snapshot("2");
        LakehouseEvent olderEvent = event("2");
        LakehouseEvent newerEvent = event("11");
        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "ICEBERG", "catalog.db.orders")).thenReturn(Optional.empty());
        when(source.scanAfter(null)).thenReturn(List.of(newer, older));
        when(eventMapper.map(IDENTITY, older)).thenReturn(olderEvent);
        when(eventMapper.map(IDENTITY, newer)).thenReturn(newerEvent);
        when(transactionService.processSnapshot(
                "catalog.db.orders", "2", NUMERIC_OFFSET_COMPARATOR, olderEvent))
                .thenReturn(result(true, olderEvent, "2"));
        when(transactionService.processSnapshot(
                "catalog.db.orders", "11", NUMERIC_OFFSET_COMPARATOR, newerEvent))
                .thenReturn(result(false, newerEvent, "11"));

        assertEquals(1, ingestionService.ingestSource(source));

        InOrder order = inOrder(transactionService);
        order.verify(transactionService).processSnapshot(
                "catalog.db.orders", "2", NUMERIC_OFFSET_COMPARATOR, olderEvent);
        order.verify(transactionService).processSnapshot(
                "catalog.db.orders", "11", NUMERIC_OFFSET_COMPARATOR, newerEvent);
    }

    /**
     * Verify the durable offset is passed to the format adapter as an opaque value.
     */
    @Test
    void ingestSourceResumesFromDurableOffset() {
        stubIdentity();
        EventConsumerOffset offset = EventConsumerOffset.builder().offsetValue("instant-42").build();
        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "ICEBERG", "catalog.db.orders")).thenReturn(Optional.of(offset));
        when(source.scanAfter("instant-42")).thenReturn(List.of());

        assertEquals(0, ingestionService.ingestSource(source));

        verify(source).scanAfter("instant-42");
    }

    /**
     * Verify later snapshots wait when projection of an earlier snapshot fails.
     */
    @Test
    void ingestSourceStopsAtFirstProjectionFailure() {
        stubIdentity();
        when(source.offsetComparator()).thenReturn(NUMERIC_OFFSET_COMPARATOR);
        LakehouseSnapshot first = snapshot("1");
        LakehouseSnapshot second = snapshot("2");
        LakehouseEvent firstEvent = event("1");
        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "ICEBERG", "catalog.db.orders")).thenReturn(Optional.empty());
        when(source.scanAfter(null)).thenReturn(List.of(first, second));
        when(eventMapper.map(IDENTITY, first)).thenReturn(firstEvent);
        when(transactionService.processSnapshot(
                "catalog.db.orders", "1", NUMERIC_OFFSET_COMPARATOR, firstEvent))
                .thenThrow(new IllegalStateException("projection failed"));

        assertEquals(0, ingestionService.ingestSource(source));

        verify(eventMapper, never()).map(IDENTITY, second);
    }

    /**
     * Verify duplicate source identities fail before they can race on one offset row.
     */
    @Test
    void ingestAllSourcesRejectsDuplicateSourceIdentity() {
        LakehouseSnapshotSource duplicate = org.mockito.Mockito.mock(LakehouseSnapshotSource.class);
        when(sourceProvider.sources()).thenReturn(List.of(source, duplicate));
        when(source.identity()).thenReturn(IDENTITY);
        when(duplicate.identity()).thenReturn(IDENTITY);

        assertThrows(IllegalStateException.class, ingestionService::ingestAllSources);

        verify(source, never()).scanAfter(any());
    }

    /**
     * Verify two lake adapters cannot project into the same logical asset key.
     */
    @Test
    void ingestAllSourcesRejectsDuplicateAssetIdentityAcrossFormats() {
        LakehouseSnapshotSource hudiSource = org.mockito.Mockito.mock(LakehouseSnapshotSource.class);
        LakehouseSourceIdentity hudiIdentity = new LakehouseSourceIdentity(
                "HUDI", "hudi-catalog.db.orders", "catalog", "db", "orders");
        when(sourceProvider.sources()).thenReturn(List.of(source, hudiSource));
        when(source.identity()).thenReturn(IDENTITY);
        when(hudiSource.identity()).thenReturn(hudiIdentity);

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                ingestionService::ingestAllSources);

        assertEquals("Duplicate lakehouse asset identity: catalog.db.orders", error.getMessage());
        verify(source, never()).scanAfter(any());
    }

    /**
     * Verify one unavailable table does not block independent lakehouse sources.
     */
    @Test
    void ingestAllSourcesIsolatesSourceScanFailures() {
        LakehouseSnapshotSource hudiSource = org.mockito.Mockito.mock(LakehouseSnapshotSource.class);
        LakehouseSourceIdentity hudiIdentity = new LakehouseSourceIdentity(
                "HUDI", "catalog.db.payments", "catalog", "db", "payments");
        when(sourceProvider.sources()).thenReturn(List.of(source, hudiSource));
        when(source.identity()).thenReturn(IDENTITY);
        when(hudiSource.identity()).thenReturn(hudiIdentity);
        when(source.offsetComparator()).thenReturn(NUMERIC_OFFSET_COMPARATOR);
        when(hudiSource.offsetComparator()).thenReturn(Comparator.naturalOrder());
        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "ICEBERG", "catalog.db.orders")).thenReturn(Optional.empty());
        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "HUDI", "catalog.db.payments")).thenReturn(Optional.empty());
        when(source.scanAfter(null)).thenThrow(new IllegalStateException("catalog unavailable"));
        when(hudiSource.scanAfter(null)).thenReturn(List.of());

        assertEquals(0, ingestionService.ingestAllSources());

        verify(hudiSource).scanAfter(null);
        verify(snapshotSourceMetrics).recordScan(
                org.mockito.ArgumentMatchers.eq(IDENTITY),
                org.mockito.ArgumentMatchers.eq("failure"),
                org.mockito.ArgumentMatchers.eq(0),
                any(java.time.Duration.class));
    }

    private void stubIdentity() {
        when(source.identity()).thenReturn(IDENTITY);
    }

    private LakehouseSnapshot snapshot(String offset) {
        return new LakehouseSnapshot(
                offset, offset, "1", null, "APPEND", true, null, null, null, null);
    }

    private LakehouseEvent event(String snapshotId) {
        return LakehouseEvent.builder()
                .eventId("event-" + snapshotId)
                .sourceType("ICEBERG")
                .snapshotId(snapshotId)
                .build();
    }

    private SnapshotIngestionTransactionService.SnapshotIngestionResult result(
            boolean inserted,
            LakehouseEvent event,
            String offset) {
        return new SnapshotIngestionTransactionService.SnapshotIngestionResult(
                inserted, event.getEventId(), offset);
    }
}
