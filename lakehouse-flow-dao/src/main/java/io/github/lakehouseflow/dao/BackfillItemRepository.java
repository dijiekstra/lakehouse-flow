package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.model.BackfillItem;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Repository for node/date items expanded from a backfill batch.
 */
@Repository
public interface BackfillItemRepository extends JpaRepository<BackfillItem, Long> {

    /**
     * Count currently blocked backfill items by stable scheduler status.
     *
     * @return grouped date-concurrency and DAG-dependency counts
     */
    @Query("SELECT i.status AS status, COUNT(i.id) AS itemCount " +
           "FROM BackfillItem i " +
           "WHERE i.status IN ('" + BackfillItemStatuses.WAITING_CONCURRENCY + "', '" +
           BackfillItemStatuses.WAITING_DEPENDENCY + "') " +
           "GROUP BY i.status")
    List<BackfillItemStatusCount> countBlockedByStatus();

    /**
     * Find the owning batch id without materializing a potentially stale item.
     *
     * @param taskInstanceId generated task scheduling intent id
     * @return owning batch id when the task belongs to a backfill
     */
    @Query("SELECT i.backfillBatchId FROM BackfillItem i WHERE i.taskInstanceId = :taskInstanceId")
    Optional<Long> findBackfillBatchIdByTaskInstanceId(@Param("taskInstanceId") Long taskInstanceId);

    /**
     * Find all items in a batch using deterministic replay order.
     *
     * @param backfillBatchId owning batch id
     * @return ordered backfill items
     */
    List<BackfillItem> findByBackfillBatchIdOrderByBizDateAscCreatedAtAsc(Long backfillBatchId);

    /**
     * Find the backfill item that owns a generated task scheduling intent.
     *
     * @param taskInstanceId generated task instance id
     * @return matching backfill item when the task came from a backfill batch
     */
    Optional<BackfillItem> findByTaskInstanceId(Long taskInstanceId);

    /**
     * Find generated task ids that are currently blocked by item or batch controls.
     *
     * @param taskInstanceIds candidate ready task ids
     * @param readyItemStatus item status required for delivery
     * @param activeBatchStatus batch status required for delivery
     * @return task ids that must be hidden from downstream ready queries
     */
    @Query("""
            SELECT i.taskInstanceId
            FROM BackfillItem i
            JOIN BackfillBatch b ON b.id = i.backfillBatchId
            WHERE i.taskInstanceId IN :taskInstanceIds
              AND (i.status <> :readyItemStatus OR b.status <> :activeBatchStatus)
            """)
    Set<Long> findUnavailableTaskInstanceIds(
            @Param("taskInstanceIds") Collection<Long> taskInstanceIds,
            @Param("readyItemStatus") String readyItemStatus,
            @Param("activeBatchStatus") String activeBatchStatus);

    /**
     * Find backfill items blocked by date admission or DAG dependency evidence.
     *
     * @param blockerType optional DAG_DEPENDENCY or DATE_CONCURRENCY filter
     * @param flowCode optional owning Flow code
     * @param targetAssetKey optional exact target or physical-table prefix
     * @param pageable bounded result page
     * @return newest matching blocked items first
     */
    @Query("""
            SELECT item
            FROM BackfillItem item, BackfillBatch batch
            WHERE batch.id = item.backfillBatchId
              AND item.status IN ('WAITING_CONCURRENCY', 'WAITING_DEPENDENCY')
              AND (:flowCode IS NULL OR batch.workflowCode = :flowCode)
              AND (:targetAssetKey IS NULL
                   OR item.targetAssetKey = :targetAssetKey
                   OR item.targetAssetKey LIKE CONCAT(:targetAssetKey, '.%'))
              AND (:blockerType IS NULL
                   OR (:blockerType = 'DATE_CONCURRENCY' AND item.status = 'WAITING_CONCURRENCY')
                   OR (:blockerType = 'DAG_DEPENDENCY' AND item.status = 'WAITING_DEPENDENCY'))
            ORDER BY item.updatedAt DESC, item.id DESC
            """)
    List<BackfillItem> findOperationalBlockers(
            @Param("blockerType") String blockerType,
            @Param("flowCode") String flowCode,
            @Param("targetAssetKey") String targetAssetKey,
            Pageable pageable);
}
