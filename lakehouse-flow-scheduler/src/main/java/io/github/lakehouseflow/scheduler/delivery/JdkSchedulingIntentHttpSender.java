package io.github.lakehouseflow.scheduler.delivery;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Java 17 HTTP client implementation for downstream scheduling intent delivery.
 */
@Component
public class JdkSchedulingIntentHttpSender implements SchedulingIntentHttpSender {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;

    /** Create a sender with the Java 17 platform HTTP client. */
    public JdkSchedulingIntentHttpSender() {
        this(HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build());
    }

    /**
     * Create a sender around a supplied client for focused tests.
     *
     * @param httpClient Java platform HTTP client
     */
    JdkSchedulingIntentHttpSender(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /** {@inheritDoc} */
    @Override
    public int post(URI destination, Map<String, String> headers, byte[] body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(destination)
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        headers.forEach(request::header);
        try {
            return httpClient.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Scheduling intent HTTP publication was interrupted", e);
        } catch (IOException e) {
            throw new IllegalStateException("Scheduling intent HTTP publication failed", e);
        }
    }
}
