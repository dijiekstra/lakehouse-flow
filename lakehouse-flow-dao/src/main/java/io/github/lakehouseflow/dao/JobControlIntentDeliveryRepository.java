package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.JobControlIntentDelivery;
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
 * Repository for job-control transport claims and delivery audit.
 */
@Repository
public interface JobControlIntentDeliveryRepository extends JpaRepository<JobControlIntentDelivery, Long> {

    /** Find the single delivery route selected for one control intent. */
    Optional<JobControlIntentDelivery> findByJobControlIntentId(Long jobControlIntentId);

    /** Lock a bounded set of due or abandoned job-control deliveries. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT delivery
            FROM JobControlIntentDelivery delivery
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
    List<JobControlIntentDelivery> findClaimableForUpdate(
            @Param("waitingStatuses") Collection<String> waitingStatuses,
            @Param("publishingStatus") String publishingStatus,
            @Param("now") LocalDateTime now,
            Pageable pageable);

    /** Lock one delivery before applying a fenced acknowledgement or failure. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT delivery FROM JobControlIntentDelivery delivery WHERE delivery.id = :id")
    Optional<JobControlIntentDelivery> findByIdForUpdate(@Param("id") Long id);

    /**
     * Count current job-control delivery rows by channel and transport-only status.
     *
     * @return grouped counts used by Micrometer backlog gauges
     */
    @Query("SELECT delivery.channel AS channel, delivery.status AS status, " +
           "COUNT(delivery) AS deliveryCount " +
           "FROM JobControlIntentDelivery delivery " +
           "GROUP BY delivery.channel, delivery.status")
    List<SchedulingIntentDeliveryStatusCount> countByChannelAndStatus();

    /**
     * Find latest exhausted job-control deliveries with scoped filters.
     *
     * @param status terminal delivery status
     * @param channel optional selected transport channel
     * @param flowCode optional Flow whose node output uses the writer table
     * @param tableAssetKey optional normalized physical table
     * @param pageable bounded result page
     * @return newest matching dead-letter rows first
     */
    @Query("""
            SELECT delivery
            FROM JobControlIntentDelivery delivery, JobControlIntent intent
            WHERE delivery.jobControlIntentId = intent.id
              AND delivery.status = :status
              AND (:channel IS NULL OR delivery.channel = :channel)
              AND (:tableAssetKey IS NULL OR intent.tableAssetKey = :tableAssetKey)
              AND (:flowCode IS NULL OR EXISTS (
                    SELECT node.id
                    FROM ScheduleNode node, FlowPlanVersion flowVersion
                    WHERE node.flowPlanVersionId = flowVersion.id
                      AND flowVersion.flowCode = :flowCode
                      AND (node.outputAssetKey = intent.tableAssetKey
                           OR node.outputAssetKey LIKE CONCAT(intent.tableAssetKey, '.%'))
              ))
            ORDER BY delivery.deadLetteredAt DESC, delivery.id DESC
            """)
    List<JobControlIntentDelivery> findDeadLetters(
            @Param("status") String status,
            @Param("channel") String channel,
            @Param("flowCode") String flowCode,
            @Param("tableAssetKey") String tableAssetKey,
            Pageable pageable);
}
