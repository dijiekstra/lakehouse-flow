package io.github.lakehouseflow.scheduler.delivery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Publishes immutable scheduling instructions through a broker-neutral gateway.
 *
 * The component becomes active only when a deployment supplies a concrete
 * {@link SchedulingIntentMessageGateway}. Lakehouse Flow therefore remains
 * independent of any single MQ product.
 */
@Component
@RequiredArgsConstructor
@ConditionalOnBean(SchedulingIntentMessageGateway.class)
public class MqSchedulingIntentPublisher implements SchedulingIntentPublisher {

    private final SchedulingIntentMessageGateway messageGateway;
    private final ObjectMapper objectMapper;

    /** {@inheritDoc} */
    @Override
    public String channel() {
        return SchedulingIntentDeliveryChannels.MQ;
    }

    /** {@inheritDoc} */
    @Override
    public void publish(SchedulingIntentPublication publication) {
        if (publication == null || !channel().equals(publication.channel())) {
            throw new IllegalArgumentException("MQ publisher requires an MQ scheduling intent publication");
        }
        if (publication.destination() == null || publication.destination().isBlank()) {
            throw new IllegalArgumentException("MQ scheduling intent destination must not be blank");
        }

        Map<String, String> headers = Map.of(
                "content-type", "application/json",
                "lakehouse-flow-intent-key", publication.intentKey(),
                "lakehouse-flow-contract-version", publication.contractVersion(),
                "lakehouse-flow-delivery-id", publication.deliveryId().toString());
        messageGateway.publish(
                publication.destination(),
                publication.intentKey(),
                headers,
                serializePayload(publication));
    }

    /** Serialize exactly the channel-neutral instruction payload. */
    private byte[] serializePayload(SchedulingIntentPublication publication) {
        try {
            return objectMapper.writeValueAsString(publication.instructionPayload())
                    .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Scheduling intent payload cannot be serialized", e);
        }
    }
}
