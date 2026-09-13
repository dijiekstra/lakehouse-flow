package io.github.lakehouseflow.integration.event;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the resilient generic snapshot scan loop.
 */
class LakehouseSnapshotScannerTest {

    /**
     * Verify a scheduled scan delegates to the format-neutral ingestion service.
     */
    @Test
    void scanSnapshotsDelegatesToIngestionService() {
        EventIngestionService ingestionService = mock(EventIngestionService.class);
        when(ingestionService.ingestAllSources()).thenReturn(2);
        LakehouseSnapshotScanner scanner = new LakehouseSnapshotScanner(ingestionService);

        scanner.scanSnapshots();

        verify(ingestionService).ingestAllSources();
    }

    /**
     * Verify a transient scan failure remains contained for the next scheduled retry.
     */
    @Test
    void scanSnapshotsContainsTransientFailure() {
        EventIngestionService ingestionService = mock(EventIngestionService.class);
        when(ingestionService.ingestAllSources()).thenThrow(new IllegalStateException("offline"));
        LakehouseSnapshotScanner scanner = new LakehouseSnapshotScanner(ingestionService);

        assertDoesNotThrow(scanner::scanSnapshots);
    }
}
