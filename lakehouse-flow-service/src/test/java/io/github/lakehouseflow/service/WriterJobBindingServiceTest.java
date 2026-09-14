package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.JobControlOperations;
import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.common.ScheduleNodeTypes;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.WriterJobBindingRepository;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.WriterJobBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests global single-writer ownership and epoch fencing without an execution engine.
 */
@ExtendWith(MockitoExtension.class)
class WriterJobBindingServiceTest {

    @Mock
    private WriterJobBindingRepository bindingRepository;

    @Mock
    private ScheduleNodeRepository scheduleNodeRepository;

    @InjectMocks
    private WriterJobBindingService service;

    /** Verify binding creation normalizes the table and engine-neutral mode set. */
    @Test
    void createBindingRegistersSoleWriter() {
        when(bindingRepository.findByWriterJobKey("writer.orders")).thenReturn(Optional.empty());
        when(bindingRepository.findByTableAssetKey("paimon.prod.orders")).thenReturn(Optional.empty());
        when(bindingRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        WriterJobBinding result = service.createBinding(
                new WriterJobBindingService.CreateWriterJobBindingCommand(
                        " writer.orders ",
                        "paimon.prod.orders.dt=2026-09-13",
                        List.of("streaming", "BATCH", "streaming")));

        assertEquals("writer.orders", result.getWriterJobKey());
        assertEquals("paimon.prod.orders", result.getTableAssetKey());
        assertEquals(List.of("STREAMING", "BATCH"), result.getAllowedProcessingModes());
        assertEquals(0L, result.getCurrentWriterEpoch());
    }

    /** Verify an identical create request is idempotent. */
    @Test
    void createBindingReturnsIdenticalExistingDefinition() {
        WriterJobBinding existing = binding(4L, null, null, null);
        when(bindingRepository.findByWriterJobKey("writer.orders")).thenReturn(Optional.of(existing));

        WriterJobBinding result = service.createBinding(
                new WriterJobBindingService.CreateWriterJobBindingCommand(
                        "writer.orders", "paimon.prod.orders", List.of("STREAMING", "BATCH")));

        assertEquals(existing, result);
        verify(bindingRepository, never()).save(any());
    }

    /** Verify another writer key cannot claim an already bound physical table. */
    @Test
    void createBindingRejectsSecondWriterForTable() {
        when(bindingRepository.findByWriterJobKey("writer.other")).thenReturn(Optional.empty());
        when(bindingRepository.findByTableAssetKey("paimon.prod.orders"))
                .thenReturn(Optional.of(binding(4L, null, null, null)));

        assertThrows(IllegalStateException.class, () -> service.createBinding(
                new WriterJobBindingService.CreateWriterJobBindingCommand(
                        "writer.other", "paimon.prod.orders", List.of("BATCH"))));
    }

    /** Verify the read method handles both blank and registered writer keys. */
    @Test
    void findBindingReturnsOnlyRegisteredWriter() {
        WriterJobBinding existing = binding(2L, null, null, null);
        when(bindingRepository.findByWriterJobKey("writer.orders")).thenReturn(Optional.of(existing));

        assertTrue(service.findBinding(" writer.orders ").isPresent());
        assertTrue(service.findBinding(" ").isEmpty());
    }

    /** Verify Flow publication accepts only a node matching its global binding. */
    @Test
    void validatePublishedNodesAcceptsMatchingWriterDefinition() {
        ScheduleNode node = node("writer.orders", "paimon.prod.orders.dt=2026-09-13", "BATCH");
        when(bindingRepository.findByWriterJobKey("writer.orders"))
                .thenReturn(Optional.of(binding(2L, null, null, null)));

        service.validatePublishedNodes(List.of(node));

        verify(bindingRepository).findByWriterJobKey("writer.orders");
    }

    /** Verify Flow publication fails closed when an output node omits writer ownership. */
    @Test
    void validatePublishedNodesRejectsMissingWriter() {
        assertThrows(IllegalArgumentException.class,
                () -> service.validatePublishedNodes(List.of(node(null, "paimon.prod.orders", "BATCH"))));
    }

    /** Verify a bounded data intent allocates a fresh epoch under the writer lock. */
    @Test
    void reserveDataIntentAllocatesBatchEpoch() {
        ScheduleNode node = node("writer.orders", "paimon.prod.orders", "BATCH");
        WriterJobBinding binding = binding(4L, null, null, null);
        when(scheduleNodeRepository.findById(8L)).thenReturn(Optional.of(node));
        when(bindingRepository.findByWriterJobKeyForUpdate("writer.orders"))
                .thenReturn(Optional.of(binding));

        WriterJobBindingService.WriterLease lease = service.reserveDataIntent(
                8L,
                "paimon.prod.orders.dt=2026-09-13",
                "BATCH",
                "task-instance:42",
                LocalDateTime.now().plusHours(1));

        assertTrue(lease.admitted());
        assertEquals(5L, lease.writerEpoch());
        assertEquals("task-instance:42", binding.getHolderIntentKey());
        assertEquals(ScheduleNodeProcessingModes.BATCH, binding.getActiveProcessingMode());
        verify(bindingRepository).save(binding);
    }

    /** Verify an unexpired bounded holder serializes different dates on the same table. */
    @Test
    void reserveDataIntentRejectsBusyBatchWriter() {
        ScheduleNode node = node("writer.orders", "paimon.prod.orders", "BATCH");
        WriterJobBinding binding = binding(
                4L, "BATCH", "task-instance:41", LocalDateTime.now().plusHours(1));
        when(scheduleNodeRepository.findById(8L)).thenReturn(Optional.of(node));
        when(bindingRepository.findByWriterJobKeyForUpdate("writer.orders"))
                .thenReturn(Optional.of(binding));

        WriterJobBindingService.WriterLease lease = service.reserveDataIntent(
                8L, "paimon.prod.orders", "BATCH", "task-instance:42", LocalDateTime.now().plusHours(1));

        assertFalse(lease.admitted());
        assertTrue(lease.rejectionReason().contains("task-instance:41"));
        verify(bindingRepository, never()).save(any());
    }

    /** Verify a streaming replay reuses the current platform-controlled generation. */
    @Test
    void reserveDataIntentReusesCurrentStreamingEpoch() {
        ScheduleNode node = node("writer.orders", "paimon.prod.orders", "STREAMING");
        WriterJobBinding binding = binding(7L, "STREAMING", "job-control:writer.orders:7", null);
        binding.setCurrentControlIntentKey("job-control:writer.orders:7");
        when(scheduleNodeRepository.findById(8L)).thenReturn(Optional.of(node));
        when(bindingRepository.findByWriterJobKeyForUpdate("writer.orders"))
                .thenReturn(Optional.of(binding));

        WriterJobBindingService.WriterLease lease = service.reserveDataIntent(
                8L, "paimon.prod.orders", "STREAMING", "task-instance:42", LocalDateTime.now().plusHours(1));

        assertTrue(lease.admitted());
        assertEquals(7L, lease.writerEpoch());
        verify(bindingRepository, never()).save(any());
    }

    /** Verify bounded writer release is fenced by intent key and epoch. */
    @Test
    void releaseDataIntentClearsOnlyCurrentBatchHolder() {
        WriterJobBinding binding = binding(
                5L, "BATCH", "task-instance:42", LocalDateTime.now().plusHours(1));
        when(bindingRepository.findByWriterJobKeyForUpdate("writer.orders"))
                .thenReturn(Optional.of(binding));

        service.releaseDataIntent("writer.orders", 5L, "task-instance:42");

        assertEquals(null, binding.getHolderIntentKey());
        assertEquals(null, binding.getActiveProcessingMode());
        verify(bindingRepository).save(binding);
    }

    /** Verify start allocates epoch one and restart advances the same writer monotonically. */
    @Test
    void allocateControlEpochSupportsStartAndRestart() {
        WriterJobBinding starting = binding(0L, null, null, null);
        WriterJobBinding restarting = binding(1L, "STREAMING", "job-control:writer.orders:1", null);
        when(bindingRepository.findByWriterJobKeyForUpdate("writer.orders"))
                .thenReturn(Optional.of(starting))
                .thenReturn(Optional.of(restarting));

        WriterJobBindingService.ControlEpochAllocation start = service.allocateControlEpoch(
                "writer.orders", JobControlOperations.START_JOB);
        WriterJobBindingService.ControlEpochAllocation restart = service.allocateControlEpoch(
                "writer.orders", JobControlOperations.RESTART_JOB);

        assertEquals(1L, start.writerEpoch());
        assertEquals(2L, restart.writerEpoch());
        assertEquals("job-control:writer.orders:2", restart.intentKey());
    }

    /** Build one writer binding fixture supporting both flow modes. */
    private WriterJobBinding binding(
            long epoch,
            String activeMode,
            String holderIntentKey,
            LocalDateTime holderExpiresAt) {
        return WriterJobBinding.builder()
                .id(3L)
                .writerJobKey("writer.orders")
                .tableAssetKey("paimon.prod.orders")
                .allowedProcessingModes(List.of("STREAMING", "BATCH"))
                .currentWriterEpoch(epoch)
                .activeProcessingMode(activeMode)
                .holderIntentKey(holderIntentKey)
                .holderExpiresAt(holderExpiresAt)
                .build();
    }

    /** Build one output node fixture referencing a writer key. */
    private ScheduleNode node(String writerJobKey, String outputAssetKey, String mode) {
        return ScheduleNode.builder()
                .id(8L)
                .nodeCode("orders")
                .nodeType(ScheduleNodeTypes.ASSET_OUTPUT)
                .processingMode(mode)
                .outputAssetKey(outputAssetKey)
                .writerJobKey(writerJobKey)
                .build();
    }
}
