package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.SchedulingIntentDeadLetterResponse;
import io.github.lakehouseflow.service.SchedulingIntentDeliveryQueryService;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the read-only scheduling-intent dead-letter API mapping.
 */
class SchedulingIntentDeliveryControllerTest {

    /** Verify transport dead letters are exposed without downstream task status. */
    @Test
    void findDeadLettersMapsOperationsEvidence() {
        SchedulingIntentDeliveryQueryService queryService = mock(SchedulingIntentDeliveryQueryService.class);
        SchedulingIntentDeliveryController controller = new SchedulingIntentDeliveryController(queryService);
        LocalDateTime deadLetteredAt = LocalDateTime.now();
        when(queryService.findDeadLetters("MQ", 25)).thenReturn(List.of(
                new SchedulingIntentDeliveryQueryService.DeadLetterDelivery(
                        201L,
                        101L,
                        "task-instance:22",
                        22L,
                        "MQ",
                        "flow-intents",
                        8,
                        "broker unavailable",
                        deadLetteredAt.minusSeconds(1),
                        deadLetteredAt.minusMinutes(1),
                        deadLetteredAt)));

        List<SchedulingIntentDeadLetterResponse> responses = controller.findDeadLetters("MQ", 25);

        assertEquals(1, responses.size());
        assertEquals("task-instance:22", responses.get(0).intentKey());
        assertEquals(deadLetteredAt, responses.get(0).deadLetteredAt());
    }
}
