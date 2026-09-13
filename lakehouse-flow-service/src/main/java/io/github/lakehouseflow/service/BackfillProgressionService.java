package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillBatchStatuses;
import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.BackfillProgressionModes;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Advances business-date admission for scheduler-side backfill batches.
 *
 * A date occupies one scheduler slot after its explicit entry-node group is
 * released and until every node/date item is snapshot-confirmed. Slot admission
 * controls intent visibility only and never represents downstream executor capacity.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class BackfillProgressionService {

    private final BackfillBatchRepository backfillBatchRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final TaskInstanceService taskInstanceService;

    /**
     * Refresh terminal batch state and release queued business dates into free slots.
     *
     * @param backfillBatchId batch whose snapshot evidence changed
     */
    public void refreshBatch(Long backfillBatchId) {
        if (backfillBatchId == null || backfillBatchId <= 0) {
            throw new IllegalArgumentException("backfillBatchId must be positive");
        }
        BackfillBatch batch = backfillBatchRepository.findByIdForUpdate(backfillBatchId)
                .orElseThrow(() -> new IllegalArgumentException("Backfill batch not found: " + backfillBatchId));
        List<BackfillItem> items = backfillItemRepository
                .findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(backfillBatchId);
        if (items.isEmpty() || BackfillBatchStatuses.CANCELLED.equals(batch.getStatus())) {
            return;
        }
        if (items.stream().anyMatch(this::snapshotNotAdvanced)) {
            updateBatchStatus(batch, BackfillBatchStatuses.FAILED);
            return;
        }
        if (items.stream().allMatch(this::snapshotConfirmed)) {
            updateBatchStatus(batch, BackfillBatchStatuses.COMPLETED);
            return;
        }
        if (!BackfillBatchStatuses.EXPANDED.equals(batch.getStatus())) {
            return;
        }

        Integer activeDateLimit = activeDateLimit(batch);
        if (activeDateLimit == null) {
            return;
        }
        releaseAvailableDates(batch, items, activeDateLimit);
    }

    /**
     * Resolve a finite scheduler-side date limit from the persisted policy.
     *
     * @param batch backfill batch
     * @return finite limit, or null for unrestricted parallel progression
     */
    private Integer activeDateLimit(BackfillBatch batch) {
        if (BackfillProgressionModes.PARALLEL.equals(batch.getProgressionMode())) {
            return null;
        }
        if (BackfillProgressionModes.SERIAL.equals(batch.getProgressionMode())) {
            return 1;
        }
        if (BackfillProgressionModes.PARALLEL_WITH_LIMIT.equals(batch.getProgressionMode())
                && batch.getMaxActiveDates() != null
                && batch.getMaxActiveDates() > 0) {
            return batch.getMaxActiveDates();
        }
        throw new IllegalStateException("Invalid backfill progression policy for batch: " + batch.getId());
    }

    /**
     * Release queued start nodes in business-date order until the limit is full.
     *
     * @param batch owning backfill batch
     * @param items all date/node items in stable order
     * @param activeDateLimit maximum admitted business dates
     */
    private void releaseAvailableDates(
            BackfillBatch batch,
            List<BackfillItem> items,
            int activeDateLimit) {

        Map<LocalDate, List<BackfillItem>> itemsByDate = items.stream()
                .collect(Collectors.groupingBy(
                        BackfillItem::getBizDate,
                        TreeMap::new,
                        Collectors.toList()));
        long activeDates = itemsByDate.values().stream()
                .filter(dateItems -> isActiveDate(batch, dateItems))
                .count();

        for (List<BackfillItem> dateItems : itemsByDate.values()) {
            if (activeDates >= activeDateLimit) {
                return;
            }
            List<BackfillItem> entryItems = findEntryItems(batch, dateItems);
            if (!entryItems.stream().allMatch(this::waitingForConcurrency)) {
                continue;
            }
            for (BackfillItem entryItem : entryItems) {
                taskInstanceService.markSchedulable(entryItem.getTaskInstanceId());
                entryItem.setStatus(BackfillItemStatuses.INTENT_READY);
                backfillItemRepository.save(entryItem);
            }
            activeDates++;
        }
    }

    /**
     * Determine whether a business date currently occupies one admission slot.
     *
     * @param batch owning batch
     * @param dateItems all selected nodes for one business date
     * @return true when the date was admitted and is not yet terminal
     */
    private boolean isActiveDate(BackfillBatch batch, List<BackfillItem> dateItems) {
        if (dateItems.stream().allMatch(this::snapshotConfirmed)) {
            return false;
        }
        return !findEntryItems(batch, dateItems).stream().allMatch(this::waitingForConcurrency);
    }

    /**
     * Find every explicit entry-node item for one business date.
     *
     * @param batch owning batch
     * @param dateItems items for one business date
     * @return entry-node items in persisted item order
     */
    private List<BackfillItem> findEntryItems(BackfillBatch batch, List<BackfillItem> dateItems) {
        Set<String> entryNodeCodes = entryNodeCodes(batch);
        List<BackfillItem> entryItems = dateItems.stream()
                .filter(item -> entryNodeCodes.contains(item.getNodeCode()))
                .toList();
        if (entryItems.size() != entryNodeCodes.size()) {
            throw new IllegalStateException("Backfill date is missing one or more entry nodes");
        }
        return entryItems;
    }

    /**
     * Resolve persisted entry nodes with compatibility for pre-unification batches.
     *
     * @param batch owning batch
     * @return distinct entry node codes
     */
    private Set<String> entryNodeCodes(BackfillBatch batch) {
        if (batch.getEntryNodeCodes() != null && !batch.getEntryNodeCodes().isEmpty()) {
            Set<String> entryNodeCodes = batch.getEntryNodeCodes().stream()
                    .filter(code -> code != null && !code.isBlank())
                    .map(String::trim)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            if (entryNodeCodes.isEmpty()) {
                throw new IllegalStateException("Backfill batch has no valid entry nodes: " + batch.getId());
            }
            return entryNodeCodes;
        }
        if (batch.getStartNodeCode() != null && !batch.getStartNodeCode().isBlank()) {
            return Set.of(batch.getStartNodeCode());
        }
        throw new IllegalStateException("Backfill batch has no entry nodes: " + batch.getId());
    }

    /**
     * Check whether an entry item is waiting for a business-date slot.
     *
     * @param item backfill entry item
     * @return true while the item is concurrency-queued
     */
    private boolean waitingForConcurrency(BackfillItem item) {
        return BackfillItemStatuses.WAITING_CONCURRENCY.equals(item.getStatus());
    }

    /**
     * Check whether an item has target snapshot confirmation.
     *
     * @param item backfill item
     * @return true when its target snapshot advanced
     */
    private boolean snapshotConfirmed(BackfillItem item) {
        return BackfillItemStatuses.SNAPSHOT_CONFIRMED.equals(item.getStatus());
    }

    /**
     * Check whether an item exhausted its confirmation window without progress.
     *
     * @param item backfill item
     * @return true when its target snapshot did not advance
     */
    private boolean snapshotNotAdvanced(BackfillItem item) {
        return BackfillItemStatuses.SNAPSHOT_NOT_ADVANCED.equals(item.getStatus());
    }

    /**
     * Persist a batch lifecycle transition only when the state changes.
     *
     * @param batch batch to update
     * @param status new scheduler-side status
     */
    private void updateBatchStatus(BackfillBatch batch, String status) {
        if (!status.equals(batch.getStatus())) {
            batch.setStatus(status);
            backfillBatchRepository.save(batch);
        }
    }
}
