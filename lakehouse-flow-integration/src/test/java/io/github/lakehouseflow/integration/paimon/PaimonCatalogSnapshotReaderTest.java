package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSnapshot;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceException;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;
import io.github.lakehouseflow.integration.source.SnapshotSourcePosition;
import io.github.lakehouseflow.integration.source.SnapshotSourceStartupMode;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests native Paimon metadata reads with SDK boundaries mocked by Mockito.
 */
@ExtendWith(MockitoExtension.class)
class PaimonCatalogSnapshotReaderTest {

    @Mock
    private PaimonCatalogFactory catalogFactory;

    @Mock
    private Catalog catalog;

    @Mock
    private FileStoreTable table;

    @Mock
    private SnapshotManager snapshotManager;

    @Mock
    private SimpleFileReader<ManifestFileMeta> manifestListReader;

    @Mock
    private SimpleFileReader<ManifestEntry> manifestFileReader;

    @Mock
    private FileStore<?> fileStore;

    @Mock
    private FileStorePathFactory pathFactory;

    private PaimonSourceDefinition definition;
    private PaimonCatalogSnapshotReader reader;

    /**
     * Configure one Paimon table source and its native catalog boundary.
     */
    @BeforeEach
    void setUp() throws Exception {
        definition = new PaimonSourceDefinition(
                new LakehouseSourceIdentity(
                        "PAIMON", "lake.ods.orders", "lake", "ods", "orders"),
                Map.of("warehouse", "file:///warehouse"),
                2,
                ZoneId.of("UTC"),
                SnapshotSourceStartupMode.EARLIEST);
        reader = new PaimonCatalogSnapshotReader(catalogFactory);
    }

    /**
     * Verify a bounded retained range includes properties and delta-manifest partitions.
     */
    @Test
    void scanAfterReadsPropertiesAndChangedPartitions() throws Exception {
        stubCatalogTable();
        when(snapshotManager.earliestSnapshotId()).thenReturn(100L);
        when(snapshotManager.latestSnapshotId()).thenReturn(102L);
        Snapshot snapshot100 = snapshot(100L, "delta-100");
        Snapshot snapshot101 = snapshot(101L, "delta-101");
        when(snapshotManager.snapshot(100L)).thenReturn(snapshot100);
        when(snapshotManager.snapshot(101L)).thenReturn(snapshot101);
        when(table.partitionKeys()).thenReturn(List.of("dt"));
        when(table.manifestListReader()).thenReturn(manifestListReader);
        when(table.manifestFileReader()).thenReturn(manifestFileReader);
        doReturn(fileStore).when(table).store();
        when(fileStore.pathFactory()).thenReturn(pathFactory);
        ManifestFileMeta manifest100 = mock(ManifestFileMeta.class);
        ManifestFileMeta manifest101 = mock(ManifestFileMeta.class);
        when(manifest100.fileName()).thenReturn("manifest-100");
        when(manifest101.fileName()).thenReturn("manifest-101");
        when(manifestListReader.read("delta-100")).thenReturn(List.of(manifest100));
        when(manifestListReader.read("delta-101")).thenReturn(List.of(manifest101));
        BinaryRow partition100 = mock(BinaryRow.class);
        BinaryRow partition101 = mock(BinaryRow.class);
        ManifestEntry entry100 = mock(ManifestEntry.class);
        ManifestEntry entry101 = mock(ManifestEntry.class);
        when(entry100.partition()).thenReturn(partition100);
        when(entry101.partition()).thenReturn(partition101);
        when(manifestFileReader.read("manifest-100")).thenReturn(List.of(entry100));
        when(manifestFileReader.read("manifest-101")).thenReturn(List.of(entry101));
        when(pathFactory.getPartitionString(partition100)).thenReturn("dt=2026-09-12/");
        when(pathFactory.getPartitionString(partition101)).thenReturn("dt=2026-09-13");

        List<LakehouseSnapshot> snapshots = reader.scanAfter(definition, null);

        assertEquals(List.of("100", "101"), snapshots.stream()
                .map(LakehouseSnapshot::sourceOffset).toList());
        assertEquals(Map.of("lakehouse-flow.intent-key", "task-instance:100"),
                snapshots.get(0).snapshotProperties());
        assertEquals(List.of("dt=2026-09-12"), snapshots.get(0).changedPartitions());
        verify(catalog).close();
    }

    /**
     * Verify an offset behind retained history fails closed instead of skipping evidence.
     */
    @Test
    void scanAfterRejectsRetentionGap() throws Exception {
        stubCatalogTable();
        when(snapshotManager.earliestSnapshotId()).thenReturn(100L);
        when(snapshotManager.latestSnapshotId()).thenReturn(105L);

        SnapshotSourceException error = assertThrows(
                SnapshotSourceException.class,
                () -> reader.scanAfter(definition, "98"));

        assertEquals(true, error.getMessage().contains("expected 99"));
        verify(snapshotManager, never()).snapshot(anyLong());
        verify(catalog).close();
    }

    /**
     * Verify an invalid persisted offset is rejected before opening the catalog.
     */
    @Test
    void scanAfterRejectsInvalidOffset() {
        assertThrows(SnapshotSourceException.class, () -> reader.scanAfter(definition, "bad-offset"));

        verify(catalogFactory, never()).openCatalog(any());
    }

    /**
     * Verify a source already at the latest snapshot produces no duplicate observations.
     */
    @Test
    void scanAfterReturnsEmptyAtLatestOffset() throws Exception {
        stubCatalogTable();
        when(snapshotManager.earliestSnapshotId()).thenReturn(100L);
        when(snapshotManager.latestSnapshotId()).thenReturn(105L);

        assertEquals(List.of(), reader.scanAfter(definition, "105"));

        verify(snapshotManager, never()).snapshot(anyLong());
    }

    /**
     * Verify the safe onboarding mode observes only the latest retained snapshot.
     */
    @Test
    void scanAfterStartsAtLatestSnapshotByDefaultMode() throws Exception {
        definition = new PaimonSourceDefinition(
                definition.identity(),
                definition.catalogOptions(),
                definition.batchSize(),
                definition.zoneId(),
                SnapshotSourceStartupMode.LATEST);
        stubCatalogTable();
        when(snapshotManager.earliestSnapshotId()).thenReturn(100L);
        when(snapshotManager.latestSnapshotId()).thenReturn(105L);
        Snapshot latestSnapshot = snapshot(105L, null);
        when(snapshotManager.snapshot(105L)).thenReturn(latestSnapshot);
        when(table.partitionKeys()).thenReturn(List.of("dt"));

        List<LakehouseSnapshot> snapshots = reader.scanAfter(definition, null);

        assertEquals(List.of("105"), snapshots.stream()
                .map(LakehouseSnapshot::snapshotId)
                .toList());
        verify(snapshotManager, never()).snapshot(100L);
    }

    /** Verify exact numeric lag and in-sync source positions are reported without manifest reads. */
    @Test
    void inspectPositionReportsLagAndCurrentOffset() throws Exception {
        stubCatalogTable();
        when(snapshotManager.earliestSnapshotId()).thenReturn(100L);
        when(snapshotManager.latestSnapshotId()).thenReturn(105L);

        SnapshotSourcePosition lagging = reader.inspectPosition(definition, "102");
        SnapshotSourcePosition current = reader.inspectPosition(definition, "105");

        assertEquals(SnapshotSourceOffsetStatus.LAGGING, lagging.status());
        assertEquals(3L, lagging.pendingOffsetCount());
        assertEquals(SnapshotSourceOffsetStatus.IN_SYNC, current.status());
        assertEquals("105", current.latestSnapshotId());
        verify(snapshotManager, never()).snapshot(anyLong());
    }

    /** Verify expired history and an offset ahead of the table fail reconciliation closed. */
    @Test
    void inspectPositionReportsBlockedOffsetRelationships() throws Exception {
        stubCatalogTable();
        when(snapshotManager.earliestSnapshotId()).thenReturn(100L);
        when(snapshotManager.latestSnapshotId()).thenReturn(105L);

        assertEquals(
                SnapshotSourceOffsetStatus.RETENTION_GAP,
                reader.inspectPosition(definition, "98").status());
        assertEquals(
                SnapshotSourceOffsetStatus.OFFSET_AHEAD,
                reader.inspectPosition(definition, "106").status());
    }

    /** Verify startup mode controls the pending count before the first durable offset. */
    @Test
    void inspectPositionHonorsStartupMode() throws Exception {
        stubCatalogTable();
        when(snapshotManager.earliestSnapshotId()).thenReturn(100L);
        when(snapshotManager.latestSnapshotId()).thenReturn(105L);

        SnapshotSourcePosition earliest = reader.inspectPosition(definition, null);
        definition = new PaimonSourceDefinition(
                definition.identity(),
                definition.catalogOptions(),
                definition.batchSize(),
                definition.zoneId(),
                SnapshotSourceStartupMode.LATEST);
        SnapshotSourcePosition latest = reader.inspectPosition(definition, null);

        assertEquals(SnapshotSourceOffsetStatus.UNINITIALIZED, earliest.status());
        assertEquals(6L, earliest.pendingOffsetCount());
        assertEquals(1L, latest.pendingOffsetCount());
    }

    private void stubCatalogTable() throws Exception {
        when(catalogFactory.openCatalog(definition.catalogOptions())).thenReturn(catalog);
        when(catalog.getTable(any())).thenReturn(table);
        when(table.snapshotManager()).thenReturn(snapshotManager);
    }

    private Snapshot snapshot(long snapshotId, String deltaManifestList) {
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.id()).thenReturn(snapshotId);
        when(snapshot.schemaId()).thenReturn(7L);
        when(snapshot.commitUser()).thenReturn("writer");
        when(snapshot.commitIdentifier()).thenReturn(snapshotId + 1_000L);
        when(snapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
        when(snapshot.timeMillis()).thenReturn(1_789_272_000_000L + snapshotId);
        when(snapshot.watermark()).thenReturn(1_789_272_000_000L);
        when(snapshot.totalRecordCount()).thenReturn(1_000L);
        when(snapshot.deltaRecordCount()).thenReturn(10L);
        when(snapshot.changelogRecordCount()).thenReturn(5L);
        when(snapshot.properties()).thenReturn(
                Map.of("lakehouse-flow.intent-key", "task-instance:" + snapshotId));
        when(snapshot.deltaManifestList()).thenReturn(deltaManifestList);
        return snapshot;
    }
}
