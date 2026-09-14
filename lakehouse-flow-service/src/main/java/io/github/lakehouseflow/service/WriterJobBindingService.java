package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.AssetKeys;
import io.github.lakehouseflow.common.JobControlOperations;
import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.WriterJobBindingRepository;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.WriterJobBinding;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

/**
 * Owns global physical-table writer bindings and fenced epoch allocation.
 *
 * The service records which writer is authorized, never whether an engine job
 * is running. Pessimistic binding locks serialize all generation changes across
 * scheduler nodes.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class WriterJobBindingService {

    private final WriterJobBindingRepository writerJobBindingRepository;
    private final ScheduleNodeRepository scheduleNodeRepository;

    /**
     * Create an immutable table-to-writer binding or return the identical existing record.
     *
     * @param command binding definition
     * @return persisted unique writer binding
     */
    public WriterJobBinding createBinding(CreateWriterJobBindingCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("writer job binding command is required");
        }
        String writerJobKey = requireText(command.writerJobKey(), "writerJobKey");
        String tableAssetKey = requireTableKey(command.tableAssetKey());
        List<String> modes = normalizeModes(command.allowedProcessingModes());

        Optional<WriterJobBinding> byKey = writerJobBindingRepository.findByWriterJobKey(writerJobKey);
        if (byKey.isPresent()) {
            requireSameDefinition(byKey.get(), tableAssetKey, modes);
            return byKey.get();
        }
        Optional<WriterJobBinding> byTable = writerJobBindingRepository.findByTableAssetKey(tableAssetKey);
        if (byTable.isPresent()) {
            throw new IllegalStateException("Physical table " + tableAssetKey
                    + " is already owned by writer " + byTable.get().getWriterJobKey());
        }
        return writerJobBindingRepository.save(WriterJobBinding.builder()
                .writerJobKey(writerJobKey)
                .tableAssetKey(tableAssetKey)
                .allowedProcessingModes(modes)
                .currentWriterEpoch(0L)
                .build());
    }

    /**
     * Find one writer binding for platform and audit APIs.
     *
     * @param writerJobKey stable platform writer key
     * @return binding when registered
     */
    @Transactional(readOnly = true)
    public Optional<WriterJobBinding> findBinding(String writerJobKey) {
        if (writerJobKey == null || writerJobKey.isBlank()) {
            return Optional.empty();
        }
        return writerJobBindingRepository.findByWriterJobKey(writerJobKey.trim());
    }

    /**
     * Validate that every output node in a version references the global sole writer.
     *
     * @param nodes complete FlowPlanVersion node set
     */
    @Transactional(readOnly = true)
    public void validatePublishedNodes(List<ScheduleNode> nodes) {
        if (nodes == null) {
            throw new IllegalArgumentException("schedule nodes are required for writer validation");
        }
        for (ScheduleNode node : nodes) {
            if (!node.hasOutputAsset()) {
                continue;
            }
            String writerJobKey = requireText(node.getWriterJobKey(),
                    "writerJobKey for output node " + node.getNodeCode());
            WriterJobBinding binding = writerJobBindingRepository.findByWriterJobKey(writerJobKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Writer job binding not found for node " + node.getNodeCode() + ": " + writerJobKey));
            requireNodeMatchesBinding(node, binding);
        }
    }

    /**
     * Reserve the writer generation required by one immutable data-processing intent.
     */
    WriterLease reserveDataIntent(
            Long scheduleNodeId,
            String targetAssetKey,
            String processingMode,
            String intentKey,
            LocalDateTime leaseExpiresAt) {
        if (scheduleNodeId == null || scheduleNodeId <= 0) {
            throw new IllegalStateException("Scheduling intent requires a published ScheduleNode writer binding");
        }
        ScheduleNode node = scheduleNodeRepository.findById(scheduleNodeId)
                .orElseThrow(() -> new IllegalStateException("ScheduleNode not found: " + scheduleNodeId));
        String writerJobKey = requireText(node.getWriterJobKey(),
                "writerJobKey for node " + node.getNodeCode());
        WriterJobBinding binding = lockBinding(writerJobKey);
        requireNodeMatchesBinding(node, binding);
        requireTargetMatchesBinding(targetAssetKey, binding);
        String mode = ScheduleNodeProcessingModes.normalize(processingMode);
        requireModeAllowed(binding, mode);

        if (ScheduleNodeProcessingModes.isStreaming(mode)) {
            if (binding.getCurrentWriterEpoch() == null || binding.getCurrentWriterEpoch() <= 0
                    || !ScheduleNodeProcessingModes.STREAMING.equals(binding.getActiveProcessingMode())
                    || isBlank(binding.getCurrentControlIntentKey())) {
                return WriterLease.rejected(writerJobKey,
                        "Streaming writer has no active platform-controlled epoch: " + writerJobKey);
            }
            return WriterLease.admitted(writerJobKey, binding.getCurrentWriterEpoch());
        }

        LocalDateTime now = LocalDateTime.now();
        if (!isBlank(binding.getHolderIntentKey())
                && !intentKey.equals(binding.getHolderIntentKey())
                && binding.getHolderExpiresAt() != null
                && now.isBefore(binding.getHolderExpiresAt())) {
            return WriterLease.rejected(writerJobKey,
                    "Writer is reserved by intent " + binding.getHolderIntentKey()
                            + " until " + binding.getHolderExpiresAt());
        }
        long nextEpoch = normalizedEpoch(binding) + 1;
        binding.setCurrentWriterEpoch(nextEpoch);
        binding.setActiveProcessingMode(ScheduleNodeProcessingModes.BATCH);
        binding.setHolderIntentKey(intentKey);
        binding.setHolderExpiresAt(leaseExpiresAt);
        binding.setCurrentControlIntentKey(null);
        writerJobBindingRepository.save(binding);
        return WriterLease.admitted(writerJobKey, nextEpoch);
    }

    /** Release a bounded data-intent holder only when its writer fencing coordinates still match. */
    void releaseDataIntent(String writerJobKey, Long writerEpoch, String intentKey) {
        if (isBlank(writerJobKey) || writerEpoch == null || writerEpoch <= 0 || isBlank(intentKey)) {
            return;
        }
        WriterJobBinding binding = lockBinding(writerJobKey.trim());
        if (writerEpoch.equals(binding.getCurrentWriterEpoch())
                && intentKey.equals(binding.getHolderIntentKey())
                && ScheduleNodeProcessingModes.BATCH.equals(binding.getActiveProcessingMode())) {
            binding.setActiveProcessingMode(null);
            binding.setHolderIntentKey(null);
            binding.setHolderExpiresAt(null);
            writerJobBindingRepository.save(binding);
        }
    }

    /** Allocate the next streaming writer generation for a platform start or restart. */
    ControlEpochAllocation allocateControlEpoch(String writerJobKey, String operationType) {
        String normalizedWriterKey = requireText(writerJobKey, "writerJobKey");
        String operation = JobControlOperations.normalize(operationType);
        WriterJobBinding binding = lockBinding(normalizedWriterKey);
        requireModeAllowed(binding, ScheduleNodeProcessingModes.STREAMING);
        long previousEpoch = normalizedEpoch(binding);
        if (JobControlOperations.START_JOB.equals(operation) && previousEpoch != 0L) {
            throw new IllegalStateException(
                    "START_JOB requires a writer with no allocated epoch; use RESTART_JOB for " + writerJobKey);
        }
        if (JobControlOperations.RESTART_JOB.equals(operation) && previousEpoch == 0L) {
            throw new IllegalStateException(
                    "RESTART_JOB requires a previously allocated writer epoch; use START_JOB for " + writerJobKey);
        }
        long writerEpoch = previousEpoch + 1;
        String intentKey = "job-control:%s:%d".formatted(normalizedWriterKey, writerEpoch);
        binding.setCurrentWriterEpoch(writerEpoch);
        binding.setActiveProcessingMode(ScheduleNodeProcessingModes.STREAMING);
        binding.setHolderIntentKey(intentKey);
        binding.setHolderExpiresAt(null);
        binding.setCurrentControlIntentKey(intentKey);
        writerJobBindingRepository.save(binding);
        return new ControlEpochAllocation(binding, intentKey, writerEpoch, previousEpoch);
    }

    /** Load and lock one registered writer binding. */
    private WriterJobBinding lockBinding(String writerJobKey) {
        return writerJobBindingRepository.findByWriterJobKeyForUpdate(writerJobKey)
                .orElseThrow(() -> new IllegalStateException("Writer job binding not found: " + writerJobKey));
    }

    /** Ensure a Flow node output and mode agree with the referenced writer definition. */
    private void requireNodeMatchesBinding(ScheduleNode node, WriterJobBinding binding) {
        String nodeTable = requireTableKey(node.getOutputAssetKey());
        if (!nodeTable.equals(binding.getTableAssetKey())) {
            throw new IllegalStateException("Node " + node.getNodeCode() + " targets " + nodeTable
                    + " but writer " + binding.getWriterJobKey() + " owns " + binding.getTableAssetKey());
        }
        requireModeAllowed(binding, ScheduleNodeProcessingModes.normalize(node.getProcessingMode()));
    }

    /** Ensure an instance-level target remains inside the physical table binding. */
    private void requireTargetMatchesBinding(String targetAssetKey, WriterJobBinding binding) {
        String targetTable = requireTableKey(targetAssetKey);
        if (!targetTable.equals(binding.getTableAssetKey())) {
            throw new IllegalStateException("Scheduling target " + targetTable
                    + " is not owned by writer " + binding.getWriterJobKey());
        }
    }

    /** Reject an engine-neutral processing mode not supported by the writer definition. */
    private void requireModeAllowed(WriterJobBinding binding, String processingMode) {
        if (binding.getAllowedProcessingModes() == null
                || !binding.getAllowedProcessingModes().contains(processingMode)) {
            throw new IllegalStateException("Writer " + binding.getWriterJobKey()
                    + " does not allow processing mode " + processingMode);
        }
    }

    /** Verify an idempotent create request has not attempted to mutate immutable ownership. */
    private void requireSameDefinition(
            WriterJobBinding binding,
            String tableAssetKey,
            List<String> allowedProcessingModes) {
        if (!tableAssetKey.equals(binding.getTableAssetKey())
                || !allowedProcessingModes.equals(binding.getAllowedProcessingModes())) {
            throw new IllegalStateException("Writer job key is already bound with a different definition: "
                    + binding.getWriterJobKey());
        }
    }

    /** Normalize, deduplicate, and require at least one supported processing mode. */
    private List<String> normalizeModes(List<String> processingModes) {
        if (processingModes == null || processingModes.isEmpty()) {
            throw new IllegalArgumentException("allowedProcessingModes must not be empty");
        }
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        processingModes.forEach(mode -> normalized.add(ScheduleNodeProcessingModes.normalize(mode)));
        return List.copyOf(normalized);
    }

    /** Normalize a table or partition key to catalog.database.table. */
    private String requireTableKey(String assetKey) {
        return AssetKeys.tableKey(assetKey)
                .orElseThrow(() -> new IllegalArgumentException(
                        "tableAssetKey must use catalog.database.table[.partition]"));
    }

    /** Require one nonblank textual identifier. */
    private String requireText(String value, String fieldName) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /** Normalize a nullable legacy epoch to zero. */
    private long normalizedEpoch(WriterJobBinding binding) {
        return binding.getCurrentWriterEpoch() == null ? 0L : binding.getCurrentWriterEpoch();
    }

    /** Check whether text is absent. */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Immutable request for registering physical-table writer ownership.
     *
     * @param writerJobKey stable platform writer routing key
     * @param tableAssetKey catalog.database.table key
     * @param allowedProcessingModes supported STREAMING and/or BATCH modes
     */
    public record CreateWriterJobBindingCommand(
            String writerJobKey,
            String tableAssetKey,
            List<String> allowedProcessingModes) {
    }

    /**
     * Result of writer admission for one data-processing intent.
     *
     * @param admitted whether the intent owns a valid writer generation
     * @param writerJobKey selected writer key
     * @param writerEpoch selected writer generation, or null when rejected
     * @param rejectionReason scheduler wait reason, or null when admitted
     */
    record WriterLease(
            boolean admitted,
            String writerJobKey,
            Long writerEpoch,
            String rejectionReason) {

        /** Build an admitted writer lease. */
        private static WriterLease admitted(String writerJobKey, long writerEpoch) {
            return new WriterLease(true, writerJobKey, writerEpoch, null);
        }

        /** Build a fail-closed writer lease rejection. */
        private static WriterLease rejected(String writerJobKey, String reason) {
            return new WriterLease(false, writerJobKey, null, reason);
        }
    }

    /**
     * Fenced generation allocated for one platform lifecycle instruction.
     *
     * @param binding locked writer ownership record
     * @param intentKey epoch-derived control intent key
     * @param writerEpoch new generation
     * @param previousWriterEpoch generation the execution plane must fence
     */
    record ControlEpochAllocation(
            WriterJobBinding binding,
            String intentKey,
            long writerEpoch,
            long previousWriterEpoch) {
    }
}
