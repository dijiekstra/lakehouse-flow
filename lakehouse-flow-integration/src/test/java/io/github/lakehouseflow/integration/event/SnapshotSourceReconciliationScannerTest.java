package io.github.lakehouseflow.integration.event;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests scheduled source reconciliation orchestration.
 */
class SnapshotSourceReconciliationScannerTest {

    /** Verify one scheduled pass delegates to bounded reconciliation repair. */
    @Test
    void reconcileSourcesDelegatesToService() {
        SnapshotSourceReconciliationService service = mock(SnapshotSourceReconciliationService.class);
        when(service.reconcileAndRepairAllSources()).thenReturn(List.of());
        SnapshotSourceReconciliationScanner scanner = new SnapshotSourceReconciliationScanner(service);

        scanner.reconcileSources();

        verify(service).reconcileAndRepairAllSources();
    }
}
