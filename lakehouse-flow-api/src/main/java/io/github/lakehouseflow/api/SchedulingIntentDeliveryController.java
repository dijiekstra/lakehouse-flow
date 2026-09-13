package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.SchedulingIntentDeadLetterResponse;
import io.github.lakehouseflow.service.SchedulingIntentDeliveryQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only operations API for scheduling-intent transport evidence.
 */
@RestController
@RequestMapping("/api/v1/scheduling-intent-deliveries")
@RequiredArgsConstructor
public class SchedulingIntentDeliveryController {

    private final SchedulingIntentDeliveryQueryService deliveryQueryService;

    /**
     * List recent delivery dead letters with an optional channel filter.
     *
     * @param channel optional DATABASE_TABLE, HTTP, or MQ channel
     * @param limit bounded row count
     * @return exhausted transport records, never downstream task failures
     */
    @GetMapping("/dead-letters")
    public List<SchedulingIntentDeadLetterResponse> findDeadLetters(
            @RequestParam(required = false) String channel,
            @RequestParam(required = false) Integer limit) {
        return deliveryQueryService.findDeadLetters(channel, limit).stream()
                .map(deadLetter -> new SchedulingIntentDeadLetterResponse(
                        deadLetter.deliveryId(),
                        deadLetter.intentId(),
                        deadLetter.intentKey(),
                        deadLetter.taskInstanceId(),
                        deadLetter.channel(),
                        deadLetter.destination(),
                        deadLetter.attemptCount(),
                        deadLetter.lastError(),
                        deadLetter.lastAttemptAt(),
                        deadLetter.deliverBefore(),
                        deadLetter.deadLetteredAt()))
                .toList();
    }
}
