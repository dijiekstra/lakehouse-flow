package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.BackfillBatch;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for persistent backfill batch envelopes.
 */
@Repository
public interface BackfillBatchRepository extends JpaRepository<BackfillBatch, Long> {

    /**
     * Find a backfill batch by its idempotency key.
     *
     * @param batchKey stable batch key
     * @return matching batch when present
     */
    Optional<BackfillBatch> findByBatchKey(String batchKey);

    /**
     * Find a backfill batch by the scheduling action key that requested it.
     *
     * @param actionKey scheduling action key
     * @return matching batch when present
     */
    Optional<BackfillBatch> findByActionKey(String actionKey);

    /**
     * Find the direct replacement created for a failed batch.
     *
     * @param sourceBackfillBatchId failed source batch id
     * @return replacement batch when recovery was already requested
     */
    Optional<BackfillBatch> findBySourceBackfillBatchId(Long sourceBackfillBatchId);

    /**
     * Lock a batch while changing its delivery-control state.
     *
     * The same lock is acquired when a backfill intent is delivered, preventing
     * pause or cancel from racing with scheduler-side delivery bookkeeping.
     *
     * @param id batch id
     * @return locked batch when present
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT b FROM BackfillBatch b WHERE b.id = :id")
    Optional<BackfillBatch> findByIdForUpdate(@Param("id") Long id);
}
