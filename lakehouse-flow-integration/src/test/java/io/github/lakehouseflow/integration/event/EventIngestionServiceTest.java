package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.integration.paimon.PaimonSnapshot;
import io.github.lakehouseflow.integration.paimon.PaimonSnapshotSource;
import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * EventIngestionService Tests
 *
 * Tests:
 * 1. Ingest new snapshots from Paimon
 * 2. Skip duplicate events (same event_id)
 * 3. Update consumer offset after successful ingestion
 * 4. Handle empty snapshots gracefully
 */
@DisplayName("Event Ingestion Service Tests")
class EventIngestionServiceTest {

    @Mock
    private PaimonSnapshotSource paimonSnapshotSource;

    @Mock
    private LakehouseEventRepository lakehouseEventRepository;

    @Mock
    private EventConsumerOffsetRepository eventConsumerOffsetRepository;

    @InjectMocks
    private EventIngestionService eventIngestionService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("Should ingest new snapshots from Paimon")
    void testIngestNewSnapshots() {
        // Arrange
        List<PaimonSnapshot> snapshots = new ArrayList<>();
        snapshots.add(PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark("2026-09-11T10:00:00")
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build());

        when(paimonSnapshotSource.getCatalogName()).thenReturn("paimon_catalog");
        when(paimonSnapshotSource.getDatabaseName()).thenReturn("ods");
        when(paimonSnapshotSource.getTableName()).thenReturn("orders");
        when(paimonSnapshotSource.scanSnapshots(null)).thenReturn(snapshots);

        LakehouseEvent expectedEvent = snapshots.get(0).toLakehouseEvent("paimon_catalog", "ods", "orders");
        when(paimonSnapshotSource.mapToLakehouseEvent(snapshots.get(0))).thenReturn(expectedEvent);

        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName("PAIMON", "paimon_catalog.ods.orders"))
                .thenReturn(Optional.empty());

        when(lakehouseEventRepository.save(any(LakehouseEvent.class))).thenReturn(expectedEvent);

        // Act
        int count = eventIngestionService.ingestFromPaimon();

        // Assert
        assertEquals(1, count);
        verify(lakehouseEventRepository, times(1)).save(any(LakehouseEvent.class));
        verify(eventConsumerOffsetRepository, times(2)).findBySourceTypeAndSourceName(anyString(), anyString());
    }

    @Test
    @DisplayName("Should skip duplicate events gracefully")
    void testSkipDuplicateEvents() {
        // Arrange
        List<PaimonSnapshot> snapshots = new ArrayList<>();
        snapshots.add(PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark("2026-09-11T10:00:00")
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build());

        when(paimonSnapshotSource.getCatalogName()).thenReturn("paimon_catalog");
        when(paimonSnapshotSource.getDatabaseName()).thenReturn("ods");
        when(paimonSnapshotSource.getTableName()).thenReturn("orders");
        when(paimonSnapshotSource.scanSnapshots(null)).thenReturn(snapshots);

        LakehouseEvent expectedEvent = snapshots.get(0).toLakehouseEvent("paimon_catalog", "ods", "orders");
        when(paimonSnapshotSource.mapToLakehouseEvent(snapshots.get(0))).thenReturn(expectedEvent);

        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName("PAIMON", "paimon_catalog.ods.orders"))
                .thenReturn(Optional.empty());

        // Simulate duplicate event (unique constraint violation)
        when(lakehouseEventRepository.save(any(LakehouseEvent.class)))
                .thenThrow(new DataIntegrityViolationException("Duplicate event_id"));

        // Act
        int count = eventIngestionService.ingestFromPaimon();

        // Assert
        assertEquals(0, count);  // No new events ingested
        verify(lakehouseEventRepository, times(1)).save(any(LakehouseEvent.class));
    }

    @Test
    @DisplayName("Should return 0 when no new snapshots")
    void testNoNewSnapshots() {
        // Arrange
        when(paimonSnapshotSource.getCatalogName()).thenReturn("paimon_catalog");
        when(paimonSnapshotSource.getDatabaseName()).thenReturn("ods");
        when(paimonSnapshotSource.getTableName()).thenReturn("orders");
        when(paimonSnapshotSource.scanSnapshots(null)).thenReturn(new ArrayList<>());

        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName("PAIMON", "paimon_catalog.ods.orders"))
                .thenReturn(Optional.empty());

        // Act
        int count = eventIngestionService.ingestFromPaimon();

        // Assert
        assertEquals(0, count);
        verify(lakehouseEventRepository, never()).save(any());
        verify(eventConsumerOffsetRepository, times(1)).findBySourceTypeAndSourceName(anyString(), anyString());
    }

    @Test
    @DisplayName("Should continue processing after single failure")
    void testContinueProcessingAfterFailure() {
        // Arrange
        List<PaimonSnapshot> snapshots = new ArrayList<>();
        snapshots.add(PaimonSnapshot.builder()
                .snapshotId("1000")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_abc")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark("2026-09-11T10:00:00")
                .deltaRecordCount(1000L)
                .changelogRecordCount(500L)
                .build());
        snapshots.add(PaimonSnapshot.builder()
                .snapshotId("1001")
                .schemaId("100")
                .commitUser("airflow")
                .commitIdentifier("commit_def")
                .commitKind("APPEND")
                .commitTime(System.currentTimeMillis())
                .watermark("2026-09-11T12:00:00")
                .deltaRecordCount(2000L)
                .changelogRecordCount(800L)
                .build());

        when(paimonSnapshotSource.getCatalogName()).thenReturn("paimon_catalog");
        when(paimonSnapshotSource.getDatabaseName()).thenReturn("ods");
        when(paimonSnapshotSource.getTableName()).thenReturn("orders");
        when(paimonSnapshotSource.scanSnapshots(null)).thenReturn(snapshots);

        LakehouseEvent event1 = snapshots.get(0).toLakehouseEvent("paimon_catalog", "ods", "orders");
        LakehouseEvent event2 = snapshots.get(1).toLakehouseEvent("paimon_catalog", "ods", "orders");

        when(paimonSnapshotSource.mapToLakehouseEvent(snapshots.get(0))).thenReturn(event1);
        when(paimonSnapshotSource.mapToLakehouseEvent(snapshots.get(1))).thenReturn(event2);

        when(eventConsumerOffsetRepository.findBySourceTypeAndSourceName("PAIMON", "paimon_catalog.ods.orders"))
                .thenReturn(Optional.empty());

        // First snapshot fails, second succeeds
        when(lakehouseEventRepository.save(event1)).thenThrow(new RuntimeException("Some error"));
        when(lakehouseEventRepository.save(event2)).thenReturn(event2);

        // Act
        int count = eventIngestionService.ingestFromPaimon();

        // Assert
        assertEquals(1, count);  // Only second snapshot ingested
        verify(lakehouseEventRepository, times(2)).save(any(LakehouseEvent.class));
    }
}
