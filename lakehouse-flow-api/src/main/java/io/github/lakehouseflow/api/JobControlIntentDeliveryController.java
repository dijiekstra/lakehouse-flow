package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.JobControlIntentDeadLetterResponse;
import io.github.lakehouseflow.service.JobControlIntentDeliveryQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only operations API for job-control intent transport evidence.
 */
@RestController
@RequestMapping("/api/v1/job-control-intent-deliveries")
@RequiredArgsConstructor
public class JobControlIntentDeliveryController {

    private final JobControlIntentDeliveryQueryService deliveryQueryService;

    /**
     * List recent job-control delivery dead letters with an optional channel filter.
     *
     * @param channel optional DATABASE_TABLE, HTTP, or MQ channel
     * @param limit bounded row count
     * @return exhausted transport records, never writer runtime failures
     */
    @GetMapping("/dead-letters")
    public List<JobControlIntentDeadLetterResponse> findDeadLetters(
            @RequestParam(required = false) String channel,
            @RequestParam(required = false) Integer limit) {
        return deliveryQueryService.findDeadLetters(channel, limit).stream()
                .map(deadLetter -> new JobControlIntentDeadLetterResponse(
                        deadLetter.deliveryId(),
                        deadLetter.intentId(),
                        deadLetter.intentKey(),
                        deadLetter.writerJobKey(),
                        deadLetter.tableAssetKey(),
                        deadLetter.operationType(),
                        deadLetter.writerEpoch(),
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
