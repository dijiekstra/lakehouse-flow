package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Repository for transport-only scheduling intent delivery evidence.
 */
@Repository
public interface SchedulingIntentDeliveryRepository extends JpaRepository<SchedulingIntentDelivery, Long> {

    /**
     * Find the database outbox delivery evidence for an intent.
     *
     * @param schedulingIntentId immutable scheduling intent id
     * @return matching delivery evidence when present
     */
    Optional<SchedulingIntentDelivery> findBySchedulingIntentId(Long schedulingIntentId);

    /**
     * Lock a bounded set of due or abandoned delivery attempts.
     *
     * The short database transaction serializes claim ownership. Network I/O is
     * performed only after this method's transaction has committed.
     *
     * @param waitingStatuses delivery states eligible when their retry time is due
     * @param publishingStatus in-flight state eligible after its claim expires
     * @param now scheduler database-time approximation used for due checks
     * @param pageable bounded deterministic claim page
     * @return locked claim candidates
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT delivery
            FROM SchedulingIntentDelivery delivery
            WHERE (
                delivery.status IN :waitingStatuses
                AND (delivery.nextAttemptAt IS NULL OR delivery.nextAttemptAt <= :now)
            ) OR (
                delivery.status = :publishingStatus
                AND delivery.claimExpiresAt IS NOT NULL
                AND delivery.claimExpiresAt <= :now
            )
            ORDER BY delivery.createdAt ASC, delivery.id ASC
            """)
    List<SchedulingIntentDelivery> findClaimableForUpdate(
            @Param("waitingStatuses") Collection<String> waitingStatuses,
            @Param("publishingStatus") String publishingStatus,
            @Param("now") LocalDateTime now,
            Pageable pageable);

    /**
     * Lock one delivery before applying a fenced completion or failure update.
     *
     * @param id delivery id
     * @return locked delivery when present
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT delivery FROM SchedulingIntentDelivery delivery WHERE delivery.id = :id")
    Optional<SchedulingIntentDelivery> findByIdForUpdate(@Param("id") Long id);

    /**
     * Count current delivery rows by channel and transport-only status.
     *
     * @return grouped counts used by Micrometer backlog gauges
     */
    @Query("SELECT delivery.channel AS channel, delivery.status AS status, " +
           "COUNT(delivery) AS deliveryCount " +
           "FROM SchedulingIntentDelivery delivery " +
           "GROUP BY delivery.channel, delivery.status")
    List<SchedulingIntentDeliveryStatusCount> countByChannelAndStatus();

    /**
     * Find the latest dead-lettered deliveries across all channels.
     *
     * @param status terminal delivery status
     * @param pageable bounded result page
     * @return newest dead-letter rows first
     */
    List<SchedulingIntentDelivery> findByStatusOrderByDeadLetteredAtDescIdDesc(
            String status,
            Pageable pageable);

    /**
     * Find the latest dead-lettered deliveries for one selected channel.
     *
     * @param status terminal delivery status
     * @param channel selected transport channel
     * @param pageable bounded result page
     * @return newest matching dead-letter rows first
     */
    List<SchedulingIntentDelivery> findByStatusAndChannelOrderByDeadLetteredAtDescIdDesc(
            String status,
            String channel,
            Pageable pageable);
}
