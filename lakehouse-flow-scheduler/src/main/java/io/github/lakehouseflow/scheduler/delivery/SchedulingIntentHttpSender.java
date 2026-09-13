package io.github.lakehouseflow.scheduler.delivery;

import java.net.URI;
import java.util.Map;

/**
 * Small HTTP transport boundary used by the scheduling intent HTTP publisher.
 */
public interface SchedulingIntentHttpSender {

    /**
     * Post one serialized scheduling intent to a downstream endpoint.
     *
     * @param destination validated HTTP or HTTPS endpoint
     * @param headers immutable transport headers
     * @param body serialized immutable instruction payload
     * @return downstream HTTP status code
     */
    int post(URI destination, Map<String, String> headers, byte[] body);
}
