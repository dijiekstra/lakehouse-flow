package io.github.lakehouseflow.dao;

/**
 * Grouped backfill-item status count used to expose stable blocking categories.
 */
public interface BackfillItemStatusCount {

    /**
     * Return the scheduler-owned backfill item status.
     *
     * @return backfill item status
     */
    String getStatus();

    /**
     * Return the number of backfill items in this status.
     *
     * @return grouped backfill item count
     */
    long getItemCount();
}
