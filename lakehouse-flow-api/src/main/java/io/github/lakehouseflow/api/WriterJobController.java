package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.CreateJobControlIntentRequest;
import io.github.lakehouseflow.api.dto.CreateWriterJobBindingRequest;
import io.github.lakehouseflow.api.dto.JobControlIntentResponse;
import io.github.lakehouseflow.api.dto.WriterJobBindingResponse;
import io.github.lakehouseflow.model.WriterJobBinding;
import io.github.lakehouseflow.service.JobControlIntentService;
import io.github.lakehouseflow.service.WriterJobBindingService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Platform API for unique writer ownership and independent lifecycle intents.
 */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class WriterJobController {

    private final WriterJobBindingService writerJobBindingService;
    private final JobControlIntentService jobControlIntentService;

    /** Register or return the immutable sole-writer definition for a table. */
    @PostMapping("/writer-jobs")
    public WriterJobBindingResponse createBinding(
            @Valid @RequestBody CreateWriterJobBindingRequest request) {
        return toBindingResponse(writerJobBindingService.createBinding(
                new WriterJobBindingService.CreateWriterJobBindingCommand(
                        request.writerJobKey(),
                        request.tableAssetKey(),
                        request.allowedProcessingModes())));
    }

    /** Read one writer ownership record without exposing engine runtime status. */
    @GetMapping("/writer-jobs/{writerJobKey}")
    public ResponseEntity<WriterJobBindingResponse> getBinding(@PathVariable String writerJobKey) {
        return writerJobBindingService.findBinding(writerJobKey)
                .map(this::toBindingResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** Allocate and deliver the first platform-controlled streaming writer epoch. */
    @PostMapping("/writer-jobs/{writerJobKey}/start")
    public JobControlIntentResponse startJob(
            @PathVariable String writerJobKey,
            @Valid @RequestBody CreateJobControlIntentRequest request) {
        return toIntentResponse(jobControlIntentService.startJob(writerJobKey, toCommand(request)));
    }

    /** Fence the previous platform writer epoch and deliver a replacement generation. */
    @PostMapping("/writer-jobs/{writerJobKey}/restart")
    public JobControlIntentResponse restartJob(
            @PathVariable String writerJobKey,
            @Valid @RequestBody CreateJobControlIntentRequest request) {
        return toIntentResponse(jobControlIntentService.restartJob(writerJobKey, toCommand(request)));
    }

    /** Read one job-control intent with independent snapshot and delivery evidence. */
    @GetMapping("/job-control-intents/{intentKey}")
    public ResponseEntity<JobControlIntentResponse> getControlIntent(@PathVariable String intentKey) {
        return jobControlIntentService.findIntent(intentKey)
                .map(this::toIntentResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** Convert one API operation request into the service command. */
    private JobControlIntentService.CreateJobControlIntentCommand toCommand(
            CreateJobControlIntentRequest request) {
        return new JobControlIntentService.CreateJobControlIntentCommand(
                request.requestKey(), request.requestedBy(), request.reason());
    }

    /** Convert writer ownership into its API representation. */
    private WriterJobBindingResponse toBindingResponse(WriterJobBinding binding) {
        return new WriterJobBindingResponse(
                binding.getId(),
                binding.getWriterJobKey(),
                binding.getTableAssetKey(),
                binding.getAllowedProcessingModes(),
                binding.getCurrentWriterEpoch(),
                binding.getActiveProcessingMode(),
                binding.getHolderIntentKey(),
                binding.getHolderExpiresAt(),
                binding.getCurrentControlIntentKey(),
                binding.getCreatedAt(),
                binding.getUpdatedAt());
    }

    /** Convert a job-control audit view into its API representation. */
    private JobControlIntentResponse toIntentResponse(JobControlIntentService.JobControlIntentView intent) {
        return new JobControlIntentResponse(
                intent.intentId(), intent.contractVersion(), intent.requestKey(), intent.intentKey(),
                intent.writerJobKey(), intent.tableAssetKey(), intent.operationType(), intent.processingMode(),
                intent.writerEpoch(), intent.previousWriterEpoch(), intent.baselineSnapshotId(),
                intent.deliverBefore(), intent.confirmationDeadline(), intent.requestedBy(), intent.reason(),
                intent.snapshotResult(), intent.observedSnapshotId(), intent.waitingReason(),
                intent.sourceHealth(), intent.sourceHealthDetail(), intent.sourceEvidenceCheckedAt(),
                intent.lastSnapshotCheckAt(), intent.instructionPayload(), intent.deliveryChannel(),
                intent.deliveryDestination(), intent.deliveryStatus(), intent.deliveryAttemptCount(),
                intent.deliveryLastError(), intent.deliveryLastAttemptAt(), intent.deliveryNextAttemptAt(),
                intent.deliveryDeadLetteredAt(), intent.publishedAt(), intent.createdAt());
    }
}
