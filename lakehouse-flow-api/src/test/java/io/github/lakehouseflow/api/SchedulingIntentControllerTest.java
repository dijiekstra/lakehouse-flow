package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.TaskSchedulingIntentResponse;
import io.github.lakehouseflow.common.SchedulingIntentContract;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.service.SchedulingIntentService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Tests the read-only scheduling-intent audit controller.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingIntentControllerTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 0, 0);
    private static final LocalDateTime PUBLISHED_AT = LocalDateTime.of(2026, 9, 12, 10, 30);

    @Mock
    private SchedulingIntentService schedulingIntentService;

    /** Verify published intent lookup returns immutable outbox evidence. */
    @Test
    void getTaskIntentReturnsPublishedIntentWhenPresent() {
        SchedulingIntentController controller = new SchedulingIntentController(schedulingIntentService);
        when(schedulingIntentService.findTaskIntent(22L)).thenReturn(Optional.of(intent()));

        ResponseEntity<TaskSchedulingIntentResponse> response = controller.getTaskIntent(22L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(101L, response.getBody().intentId());
        assertEquals("task-instance:22", response.getBody().intentKey());
        assertEquals(SchedulingIntentContract.CONTRACT_VERSION, response.getBody().contractVersion());
        assertEquals("SNAPSHOT", response.getBody().triggerType());
        assertEquals("task-instance:22", response.getBody().instructionPayload().get("intentKey"));
        assertEquals(SchedulingIntentDeliveryChannels.DATABASE_TABLE, response.getBody().deliveryChannel());
        assertEquals(SchedulingIntentDeliveryStatuses.PUBLISHED, response.getBody().deliveryStatus());
        assertEquals(1, response.getBody().deliveryAttemptCount());
    }

    /** Verify unpublished task lookup returns 404 without a mutation fallback. */
    @Test
    void getTaskIntentReturnsNotFoundWhenAbsent() {
        SchedulingIntentController controller = new SchedulingIntentController(schedulingIntentService);
        when(schedulingIntentService.findTaskIntent(404L)).thenReturn(Optional.empty());

        ResponseEntity<TaskSchedulingIntentResponse> response = controller.getTaskIntent(404L);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    /** Build one immutable published intent fixture. */
    private SchedulingIntentService.TaskSchedulingIntent intent() {
        return new SchedulingIntentService.TaskSchedulingIntent(
                101L,
                SchedulingIntentContract.CONTRACT_VERSION,
                "task-instance:22",
                22L,
                11L,
                "SNAPSHOT",
                null,
                null,
                "node.dwd_orders",
                3,
                51L,
                61L,
                BIZ_DATE,
                "paimon.dwd.orders",
                "100",
                "writer.dwd.orders",
                3L,
                "BATCH",
                List.of(),
                Map.of("intentKey", "task-instance:22"),
                SchedulingIntentDeliveryChannels.DATABASE_TABLE,
                "scheduling_intent",
                SchedulingIntentDeliveryStatuses.PUBLISHED,
                1,
                null,
                PUBLISHED_AT,
                null,
                null,
                PUBLISHED_AT,
                PUBLISHED_AT);
    }
}
