package io.github.lakehouseflow.scheduler.delivery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pushes the immutable scheduling instruction to a downstream HTTP endpoint.
 *
 * A 2xx response is transport evidence only. Response bodies and downstream
 * execution status are intentionally ignored.
 */
@Component
@RequiredArgsConstructor
public class HttpSchedulingIntentPublisher implements SchedulingIntentPublisher {

    private final SchedulingIntentHttpSender httpSender;
    private final ObjectMapper objectMapper;

    /** {@inheritDoc} */
    @Override
    public String channel() {
        return SchedulingIntentDeliveryChannels.HTTP;
    }

    /** {@inheritDoc} */
    @Override
    public void publish(SchedulingIntentPublication publication) {
        requireChannel(publication);
        URI destination = requireHttpDestination(publication.destination());
        byte[] body = serializePayload(publication).getBytes(StandardCharsets.UTF_8);
        int statusCode = httpSender.post(destination, transportHeaders(publication), body);
        if (statusCode < 200 || statusCode >= 300) {
            throw new IllegalStateException("Scheduling intent HTTP endpoint returned status " + statusCode);
        }
    }

    /** Require an HTTP publication routed to this publisher. */
    private void requireChannel(SchedulingIntentPublication publication) {
        if (publication == null || !channel().equals(publication.channel())) {
            throw new IllegalArgumentException("HTTP publisher requires an HTTP scheduling intent publication");
        }
    }

    /** Parse and validate the selected HTTP endpoint. */
    private URI requireHttpDestination(String destination) {
        try {
            URI uri = URI.create(destination);
            if (!uri.isAbsolute() || !("http".equalsIgnoreCase(uri.getScheme())
                    || "https".equalsIgnoreCase(uri.getScheme()))) {
                throw new IllegalArgumentException("HTTP destination must use http or https");
            }
            return uri;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid scheduling intent HTTP destination: " + destination, e);
        }
    }

    /** Serialize exactly the channel-neutral instruction payload. */
    private String serializePayload(SchedulingIntentPublication publication) {
        try {
            return objectMapper.writeValueAsString(publication.instructionPayload());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Scheduling intent payload cannot be serialized", e);
        }
    }

    /** Build transport metadata without adding any task runtime status. */
    private Map<String, String> transportHeaders(SchedulingIntentPublication publication) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Idempotency-Key", publication.intentKey());
        headers.put("X-Lakehouse-Flow-Intent-Key", publication.intentKey());
        headers.put("X-Lakehouse-Flow-Contract-Version", publication.contractVersion());
        headers.put("X-Lakehouse-Flow-Delivery-Id", publication.deliveryId().toString());
        return Map.copyOf(headers);
    }
}
