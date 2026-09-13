package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.service.SchedulingIntentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the internal scanner that publishes ready decisions to the outbox.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingIntentOutboxScannerTest {

    @Mock
    private SchedulingIntentService schedulingIntentService;

    @InjectMocks
    private SchedulingIntentOutboxScanner schedulingIntentOutboxScanner;

    /** Configure the publication batch size used by the scanner. */
    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(schedulingIntentOutboxScanner, "batchSize", 25);
    }

    /** Verify each scan asks the service to publish one bounded internal batch. */
    @Test
    void publishReadyIntentsUsesConfiguredBatchSize() {
        when(schedulingIntentService.publishReadyTaskIntents(25)).thenReturn(List.of());

        schedulingIntentOutboxScanner.publishReadyIntents();

        verify(schedulingIntentService).publishReadyTaskIntents(25);
    }
}
