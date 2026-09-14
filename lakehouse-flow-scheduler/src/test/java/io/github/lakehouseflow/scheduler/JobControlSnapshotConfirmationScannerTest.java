package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.service.JobControlSnapshotConfirmationService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests periodic snapshot-only confirmation for platform writer generations.
 */
@ExtendWith(MockitoExtension.class)
class JobControlSnapshotConfirmationScannerTest {

    @Mock
    private JobControlSnapshotConfirmationService confirmationService;

    @InjectMocks
    private JobControlSnapshotConfirmationScanner scanner;

    /** Verify the scanner delegates only to waiting control-intent reconciliation. */
    @Test
    void checkWaitingIntentsDelegatesToSnapshotConfirmation() {
        when(confirmationService.checkWaitingIntents()).thenReturn(List.of());

        scanner.checkWaitingIntents();

        verify(confirmationService).checkWaitingIntents();
    }
}
