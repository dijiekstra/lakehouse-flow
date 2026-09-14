package io.github.lakehouseflow.scheduler.delivery;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the Java 17 HTTP transport timeout and validation boundary. */
class JdkSchedulingIntentHttpSenderTest {

    /** Verify the configured timeout is attached to every outbound request. */
    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void postUsesConfiguredRequestTimeout() throws Exception {
        HttpClient client = mock(HttpClient.class);
        HttpResponse response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(202);
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        JdkSchedulingIntentHttpSender sender =
                new JdkSchedulingIntentHttpSender(client, Duration.ofMillis(125));

        int status = sender.post(
                URI.create("http://127.0.0.1/intents"),
                Map.of("Content-Type", "application/json"),
                "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        ArgumentCaptor<HttpRequest> request = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client).send(request.capture(), any(HttpResponse.BodyHandler.class));
        assertEquals(202, status);
        assertEquals(Duration.ofMillis(125), request.getValue().timeout().orElseThrow());
    }

    /** Verify zero, negative, and absent request timeouts fail during construction. */
    @Test
    void constructorRejectsInvalidTimeout() {
        HttpClient client = mock(HttpClient.class);

        assertThrows(IllegalArgumentException.class,
                () -> new JdkSchedulingIntentHttpSender(client, Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> new JdkSchedulingIntentHttpSender(client, Duration.ofMillis(-1)));
        assertThrows(IllegalArgumentException.class,
                () -> new JdkSchedulingIntentHttpSender(client, null));
    }
}
