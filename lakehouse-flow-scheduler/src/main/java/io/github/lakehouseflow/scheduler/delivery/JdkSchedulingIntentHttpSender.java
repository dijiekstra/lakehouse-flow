package io.github.lakehouseflow.scheduler.delivery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;
    private final Duration requestTimeout;

    /**
     * Create a sender with a deployment-configurable Java 17 HTTP timeout.
     *
     * @param requestTimeout connect and complete-request timeout
     */
    @Autowired
    public JdkSchedulingIntentHttpSender(
            @Value("${lakehouse-flow.scheduling-intent-delivery.http.request-timeout:PT30S}")
            Duration requestTimeout) {
        this(HttpClient.newBuilder()
                .connectTimeout(requirePositive(requestTimeout))
                .build(), requestTimeout);
    }

    /**
     * Create a sender around a supplied client for focused tests.
     *
     * @param httpClient Java platform HTTP client
     */
    JdkSchedulingIntentHttpSender(HttpClient httpClient) {
        this(httpClient, DEFAULT_REQUEST_TIMEOUT);
    }

    /** Build a focused sender with an explicit request timeout. */
    JdkSchedulingIntentHttpSender(HttpClient httpClient, Duration requestTimeout) {
        this.httpClient = httpClient;
        this.requestTimeout = requirePositive(requestTimeout);
    }

    /** {@inheritDoc} */
    @Override
    public int post(URI destination, Map<String, String> headers, byte[] body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(destination)
                .timeout(requestTimeout)
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

    /** Require a positive transport timeout before constructing the HTTP client. */
    private static Duration requirePositive(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("HTTP request timeout must be positive");
        }
        return timeout;
    }
}
