package io.github.lakehouseflow.scheduler.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * Tests broker-neutral MQ scheduling intent publication.
 */
@ExtendWith(MockitoExtension.class)
class MqSchedulingIntentPublisherTest {

    @Mock
    private SchedulingIntentMessageGateway messageGateway;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private MqSchedulingIntentPublisher publisher;

    /** Verify MQ publication preserves an older same-major version and exact payload. */
    @Test
    @SuppressWarnings("unchecked")
    void publishSendsImmutableInstructionPayload() {
        SchedulingIntentPublication publication = publication("lakehouse-flow.intents");

        publisher.publish(publication);

        ArgumentCaptor<Map<String, String>> headers = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(messageGateway).publish(
                eq("lakehouse-flow.intents"),
                eq("task-instance:22"),
                headers.capture(),
                payload.capture());
        assertEquals("1.2", headers.getValue().get("lakehouse-flow-contract-version"));
        assertEquals("{\"intentKey\":\"task-instance:22\"}",
                new String(payload.getValue(), StandardCharsets.UTF_8));
    }

    /** Verify an MQ route without a destination is rejected before broker access. */
    @Test
    void publishRejectsBlankDestination() {
        assertThrows(IllegalArgumentException.class, () -> publisher.publish(publication(" ")));
    }

    /** Verify the publisher declares the MQ route it handles. */
    @Test
    void channelReturnsMq() {
        assertEquals(SchedulingIntentDeliveryChannels.MQ, publisher.channel());
    }

    /** Build one immutable MQ publication fixture. */
    private SchedulingIntentPublication publication(String destination) {
        return new SchedulingIntentPublication(
                201L,
                "claim-token",
                SchedulingIntentDeliveryChannels.MQ,
                destination,
                101L,
                "task-instance:22",
                "1.2",
                Map.of("intentKey", "task-instance:22"));
    }
}
