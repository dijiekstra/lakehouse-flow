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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests HTTP scheduling intent publication without performing network I/O.
 */
@ExtendWith(MockitoExtension.class)
class HttpSchedulingIntentPublisherTest {

    @Mock
    private SchedulingIntentHttpSender httpSender;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private HttpSchedulingIntentPublisher publisher;

    /** Verify HTTP publication preserves an older same-major version and idempotency metadata. */
    @Test
    @SuppressWarnings("unchecked")
    void publishPostsImmutableInstructionPayload() {
        SchedulingIntentPublication publication = publication("https://downstream.example/intents");
        when(httpSender.post(any(URI.class), any(), any())).thenReturn(202);

        publisher.publish(publication);

        ArgumentCaptor<Map<String, String>> headers = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<byte[]> body = ArgumentCaptor.forClass(byte[].class);
        verify(httpSender).post(
                eq(URI.create("https://downstream.example/intents")),
                headers.capture(),
                body.capture());
        assertEquals("task-instance:22", headers.getValue().get("Idempotency-Key"));
        assertEquals("task-instance:22", headers.getValue().get("X-Lakehouse-Flow-Intent-Key"));
        assertEquals("1.2", headers.getValue().get("X-Lakehouse-Flow-Contract-Version"));
        assertEquals("201", headers.getValue().get("X-Lakehouse-Flow-Delivery-Id"));
        assertEquals("{\"intentKey\":\"task-instance:22\"}",
                new String(body.getValue(), StandardCharsets.UTF_8));
    }

    /** Verify non-2xx transport responses are surfaced for retry handling. */
    @Test
    void publishRejectsNonSuccessfulHttpStatus() {
        when(httpSender.post(any(), any(), any())).thenReturn(503);

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> publisher.publish(publication("https://downstream.example/intents")));

        assertEquals("Scheduling intent HTTP endpoint returned status 503", error.getMessage());
    }

    /** Verify an unsafe or relative destination is rejected before transport. */
    @Test
    void publishRejectsInvalidDestination() {
        assertThrows(IllegalArgumentException.class, () -> publisher.publish(publication("file:///tmp/intent")));
    }

    /** Verify the publisher declares the HTTP route it handles. */
    @Test
    void channelReturnsHttp() {
        assertEquals(SchedulingIntentDeliveryChannels.HTTP, publisher.channel());
    }

    /** Build one immutable HTTP publication fixture. */
    private SchedulingIntentPublication publication(String destination) {
        return new SchedulingIntentPublication(
                201L,
                "claim-token",
                SchedulingIntentDeliveryChannels.HTTP,
                destination,
                101L,
                "task-instance:22",
                "1.2",
                Map.of("intentKey", "task-instance:22"));
    }
}
