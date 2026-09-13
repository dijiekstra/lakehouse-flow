package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.model.SnapshotConfirmationResult;
import io.github.lakehouseflow.service.SnapshotConfirmationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the scheduled scanner that confirms tasks from snapshot progress.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotConfirmationScannerTest {

    @Mock
    private SnapshotConfirmationService snapshotConfirmationService;

    @InjectMocks
    private SnapshotConfirmationScanner snapshotConfirmationScanner;

    private final Duration confirmationTimeout = Duration.ofMinutes(30);

    /**
     * Inject scheduler configuration used by the scanner under test.
     */
    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(snapshotConfirmationScanner, "confirmationTimeout", confirmationTimeout);
    }

    /**
     * Verify the scanner delegates with the configured confirmation timeout.
     */
    @Test
    void scansScheduledTasksWithConfiguredConfirmationTimeout() {
        when(snapshotConfirmationService.checkScheduledTasks(confirmationTimeout))
                .thenReturn(List.of(new SnapshotConfirmationResult(
                        1L,
                        "paimon.prod.dwd_orders",
                        "100",
                        "101",
                        SchedulingStates.SNAPSHOT_CONFIRMED,
                        true,
                        false,
                        null)));

        snapshotConfirmationScanner.scanScheduledTasks();

        verify(snapshotConfirmationService).checkScheduledTasks(confirmationTimeout);
    }
}
