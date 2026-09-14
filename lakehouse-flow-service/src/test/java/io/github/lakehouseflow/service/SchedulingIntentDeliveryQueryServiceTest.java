package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests bounded dead-letter operations queries without changing transport state.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingIntentDeliveryQueryServiceTest {

    @Mock
    private SchedulingIntentDeliveryRepository deliveryRepository;

    @Mock
    private SchedulingIntentRepository intentRepository;

    @Mock
    private WorkflowInstanceRepository workflowInstanceRepository;

    @InjectMocks
    private SchedulingIntentDeliveryQueryService queryService;

    /** Verify the default query returns joined immutable intent evidence. */
    @Test
    void findDeadLettersAcrossChannels() {
        SchedulingIntentDelivery delivery = delivery();
        SchedulingIntent intent = intent();
        when(deliveryRepository.findDeadLetters(
                SchedulingIntentDeliveryStatuses.EXHAUSTED,
                null,
                null,
                null,
                PageRequest.of(0, 100)))
                .thenReturn(List.of(delivery));
        when(intentRepository.findAllById(List.of(101L))).thenReturn(List.of(intent));
        when(workflowInstanceRepository.findAllById(List.of(11L))).thenReturn(List.of(workflow()));

        List<SchedulingIntentDeliveryQueryService.DeadLetterDelivery> result =
                queryService.findDeadLetters(null, null);

        assertEquals(1, result.size());
        assertEquals("task-instance:22", result.get(0).intentKey());
        assertEquals("flow.orders", result.get(0).flowCode());
        assertEquals("lake.dwd.orders", result.get(0).targetAssetKey());
        assertEquals("broker unavailable", result.get(0).lastError());
    }

    /** Verify channel filters are normalized before querying. */
    @Test
    void findDeadLettersByChannel() {
        when(deliveryRepository.findDeadLetters(
                SchedulingIntentDeliveryStatuses.EXHAUSTED,
                SchedulingIntentDeliveryChannels.MQ,
                "flow.orders",
                "lake.dwd.orders",
                PageRequest.of(0, 25)))
                .thenReturn(List.of());
        when(intentRepository.findAllById(List.of())).thenReturn(List.of());
        when(workflowInstanceRepository.findAllById(List.of())).thenReturn(List.of());

        assertEquals(0, queryService.findDeadLetters(
                " mq ", " flow.orders ", " lake.dwd.orders ", 25).size());

        verify(deliveryRepository).findDeadLetters(
                SchedulingIntentDeliveryStatuses.EXHAUSTED,
                SchedulingIntentDeliveryChannels.MQ,
                "flow.orders",
                "lake.dwd.orders",
                PageRequest.of(0, 25));
    }

    /** Verify unsupported filters and unbounded requests fail early. */
    @Test
    void findDeadLettersRejectsInvalidFilters() {
        assertThrows(IllegalArgumentException.class, () -> queryService.findDeadLetters("FILE", 10));
        assertThrows(IllegalArgumentException.class, () -> queryService.findDeadLetters(null, 0));
        assertThrows(IllegalArgumentException.class, () -> queryService.findDeadLetters(null, 501));
    }

    /** Build one exhausted transport fixture. */
    private SchedulingIntentDelivery delivery() {
        return SchedulingIntentDelivery.builder()
                .id(201L)
                .schedulingIntentId(101L)
                .channel(SchedulingIntentDeliveryChannels.MQ)
                .destination("flow-intents")
                .status(SchedulingIntentDeliveryStatuses.EXHAUSTED)
                .attemptCount(8)
                .lastError("broker unavailable")
                .deadLetteredAt(LocalDateTime.now())
                .build();
    }

    /** Build the immutable scheduling intent joined into the operations view. */
    private SchedulingIntent intent() {
        return SchedulingIntent.builder()
                .id(101L)
                .intentKey("task-instance:22")
                .taskInstanceId(22L)
                .workflowInstanceId(11L)
                .taskCode("node.dwd_orders")
                .targetAssetKey("lake.dwd.orders")
                .bizDate(LocalDateTime.of(2026, 9, 14, 0, 0))
                .build();
    }

    /** Build the workflow identity joined into the dead-letter view. */
    private WorkflowInstance workflow() {
        return WorkflowInstance.builder()
                .id(11L)
                .workflowCode("flow.orders")
                .build();
    }
}
