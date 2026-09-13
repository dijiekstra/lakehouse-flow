package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests scheduler-side business-date admission for backfill batches.
 */
@ExtendWith(MockitoExtension.class)
class BackfillProgressionServiceTest {

    private static final LocalDate DAY_ONE = LocalDate.of(2026, 9, 10);
    private static final LocalDate DAY_TWO = LocalDate.of(2026, 9, 11);

    @Mock
    private BackfillBatchRepository backfillBatchRepository;

    @Mock
    private BackfillItemRepository backfillItemRepository;

    @Mock
    private TaskInstanceService taskInstanceService;

    @InjectMocks
    private BackfillProgressionService backfillProgressionService;

    /**
     * Verify a completed serial date releases the next queued start node.
     */
    @Test
    void refreshBatchReleasesNextSerialDateAfterSnapshotConfirmation() {
        BackfillBatch batch = batch(BackfillBatchStatuses.EXPANDED, BackfillProgressionModes.SERIAL, 1);
        BackfillItem completed = item(91L, 71L, DAY_ONE, BackfillItemStatuses.SNAPSHOT_CONFIRMED);
        BackfillItem queued = item(92L, 72L, DAY_TWO, BackfillItemStatuses.WAITING_CONCURRENCY);
        stubBatch(batch, List.of(completed, queued));

        backfillProgressionService.refreshBatch(81L);

        verify(taskInstanceService).markSchedulable(72L);
        assertEquals(BackfillItemStatuses.INTENT_READY, queued.getStatus());
        verify(backfillItemRepository).save(queued);
        verify(backfillBatchRepository, never()).save(batch);
    }

    /**
     * Verify every root of a complete-Flow date is released within one serial slot.
     */
    @Test
    void refreshBatchReleasesAllEntryNodesForCompleteFlowDate() {
        BackfillBatch batch = batch(BackfillBatchStatuses.EXPANDED, BackfillProgressionModes.SERIAL, 1);
        batch.setEntryNodeCodes(List.of("node.ods_orders", "node.ods_payments"));
        BackfillItem completedOrders = item(
                91L, 71L, DAY_ONE, "node.ods_orders", BackfillItemStatuses.SNAPSHOT_CONFIRMED);
        BackfillItem completedPayments = item(
                92L, 72L, DAY_ONE, "node.ods_payments", BackfillItemStatuses.SNAPSHOT_CONFIRMED);
        BackfillItem queuedOrders = item(
                93L, 73L, DAY_TWO, "node.ods_orders", BackfillItemStatuses.WAITING_CONCURRENCY);
        BackfillItem queuedPayments = item(
                94L, 74L, DAY_TWO, "node.ods_payments", BackfillItemStatuses.WAITING_CONCURRENCY);
        BackfillItem waitingJoin = item(
                95L, 75L, DAY_TWO, "node.dwd_order_payments", BackfillItemStatuses.WAITING_DEPENDENCY);
        stubBatch(batch, List.of(
                completedOrders,
                completedPayments,
                queuedOrders,
                queuedPayments,
                waitingJoin));

        backfillProgressionService.refreshBatch(81L);

        verify(taskInstanceService).markSchedulable(73L);
        verify(taskInstanceService).markSchedulable(74L);
        assertEquals(BackfillItemStatuses.INTENT_READY, queuedOrders.getStatus());
        assertEquals(BackfillItemStatuses.INTENT_READY, queuedPayments.getStatus());
        verify(backfillItemRepository).save(queuedOrders);
        verify(backfillItemRepository).save(queuedPayments);
    }

    /**
     * Verify an active date keeps a serial successor in the concurrency queue.
     */
    @Test
    void refreshBatchKeepsQueuedDateWhileSerialSlotIsActive() {
        BackfillBatch batch = batch(BackfillBatchStatuses.EXPANDED, BackfillProgressionModes.SERIAL, 1);
        BackfillItem active = item(91L, 71L, DAY_ONE, BackfillItemStatuses.INTENT_DELIVERED);
        BackfillItem queued = item(92L, 72L, DAY_TWO, BackfillItemStatuses.WAITING_CONCURRENCY);
        stubBatch(batch, List.of(active, queued));

        backfillProgressionService.refreshBatch(81L);

        verifyNoInteractions(taskInstanceService);
        verify(backfillItemRepository, never()).save(queued);
    }

    /**
     * Verify limited parallel progression fills every available date slot in order.
     */
    @Test
    void refreshBatchFillsLimitedParallelDateSlots() {
        BackfillBatch batch = batch(
                BackfillBatchStatuses.EXPANDED,
                BackfillProgressionModes.PARALLEL_WITH_LIMIT,
                2);
        BackfillItem active = item(91L, 71L, DAY_ONE, BackfillItemStatuses.INTENT_READY);
        BackfillItem queued = item(92L, 72L, DAY_TWO, BackfillItemStatuses.WAITING_CONCURRENCY);
        stubBatch(batch, List.of(active, queued));

        backfillProgressionService.refreshBatch(81L);

        verify(taskInstanceService).markSchedulable(72L);
        assertEquals(BackfillItemStatuses.INTENT_READY, queued.getStatus());
    }

    /**
     * Verify a non-advancing node marks the batch failed and stops slot admission.
     */
    @Test
    void refreshBatchMarksBatchFailedWhenSnapshotDoesNotAdvance() {
        BackfillBatch batch = batch(BackfillBatchStatuses.EXPANDED, BackfillProgressionModes.SERIAL, 1);
        BackfillItem failed = item(91L, 71L, DAY_ONE, BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED);
        BackfillItem queued = item(92L, 72L, DAY_TWO, BackfillItemStatuses.WAITING_CONCURRENCY);
        stubBatch(batch, List.of(failed, queued));

        backfillProgressionService.refreshBatch(81L);

        assertEquals(BackfillBatchStatuses.FAILED, batch.getStatus());
        verify(backfillBatchRepository).save(batch);
        verifyNoInteractions(taskInstanceService);
    }

    /**
     * Verify a batch completes only after every date/node item is snapshot-confirmed.
     */
    @Test
    void refreshBatchMarksBatchCompletedWhenAllSnapshotsConfirm() {
        BackfillBatch batch = batch(BackfillBatchStatuses.EXPANDED, BackfillProgressionModes.SERIAL, 1);
        stubBatch(batch, List.of(
                item(91L, 71L, DAY_ONE, BackfillItemStatuses.SNAPSHOT_CONFIRMED),
                item(92L, 72L, DAY_TWO, BackfillItemStatuses.SNAPSHOT_CONFIRMED)));

        backfillProgressionService.refreshBatch(81L);

        assertEquals(BackfillBatchStatuses.COMPLETED, batch.getStatus());
        verify(backfillBatchRepository).save(batch);
        verifyNoInteractions(taskInstanceService);
    }

    /**
     * Verify a paused batch does not admit queued dates until it is resumed.
     */
    @Test
    void refreshBatchDoesNotReleaseDatesWhilePaused() {
        BackfillBatch batch = batch(BackfillBatchStatuses.PAUSED, BackfillProgressionModes.SERIAL, 1);
        BackfillItem completed = item(91L, 71L, DAY_ONE, BackfillItemStatuses.SNAPSHOT_CONFIRMED);
        BackfillItem queued = item(92L, 72L, DAY_TWO, BackfillItemStatuses.WAITING_CONCURRENCY);
        stubBatch(batch, List.of(completed, queued));

        backfillProgressionService.refreshBatch(81L);

        verifyNoInteractions(taskInstanceService);
        verify(backfillItemRepository, never()).save(queued);
    }

    /**
     * Verify invalid identifiers fail before accessing persistence.
     */
    @Test
    void refreshBatchRejectsInvalidBatchId() {
        assertThrows(IllegalArgumentException.class, () -> backfillProgressionService.refreshBatch(0L));

        verifyNoInteractions(backfillBatchRepository, backfillItemRepository, taskInstanceService);
    }

    /**
     * Stub a locked batch and its ordered node/date items.
     *
     * @param batch batch fixture
     * @param items item fixtures
     */
    private void stubBatch(BackfillBatch batch, List<BackfillItem> items) {
        when(backfillBatchRepository.findByIdForUpdate(81L)).thenReturn(Optional.of(batch));
        when(backfillItemRepository.findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(81L))
                .thenReturn(items);
    }

    /**
     * Build a backfill batch fixture.
     *
     * @param status scheduler-side batch status
     * @param mode date progression mode
     * @param maxActiveDates finite active-date limit
     * @return batch fixture
     */
    private BackfillBatch batch(String status, String mode, Integer maxActiveDates) {
        return BackfillBatch.builder()
                .id(81L)
                .startNodeCode("node.dwd_orders")
                .status(status)
                .progressionMode(mode)
                .maxActiveDates(maxActiveDates)
                .build();
    }

    /**
     * Build one start-node item for a business date.
     *
     * @param id item id
     * @param taskInstanceId task scheduling instance id
     * @param bizDate business date
     * @param status scheduler-side item status
     * @return item fixture
     */
    private BackfillItem item(Long id, Long taskInstanceId, LocalDate bizDate, String status) {
        return item(id, taskInstanceId, bizDate, "node.dwd_orders", status);
    }

    /**
     * Build one named node item for a business date.
     *
     * @param id item id
     * @param taskInstanceId task scheduling instance id
     * @param bizDate business date
     * @param nodeCode selected node code
     * @param status scheduler-side item status
     * @return item fixture
     */
    private BackfillItem item(
            Long id,
            Long taskInstanceId,
            LocalDate bizDate,
            String nodeCode,
            String status) {

        return BackfillItem.builder()
                .id(id)
                .backfillBatchId(81L)
                .bizDate(bizDate)
                .nodeCode(nodeCode)
                .taskInstanceId(taskInstanceId)
                .status(status)
                .build();
    }
}
