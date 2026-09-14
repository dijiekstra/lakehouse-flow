package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.JobControlIntent;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for immutable writer lifecycle instructions and snapshot evidence.
 */
@Repository
public interface JobControlIntentRepository extends JpaRepository<JobControlIntent, Long> {

    /**
     * Lock one control intent while snapshot evidence is evaluated and persisted.
     *
     * @param id immutable control intent id
     * @return locked control intent when present
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT intent FROM JobControlIntent intent WHERE intent.id = :id")
    Optional<JobControlIntent> findByIdForUpdate(@Param("id") Long id);

    /** Find one control intent by its epoch-derived idempotency key. */
    Optional<JobControlIntent> findByIntentKey(String intentKey);

    /** Find one control intent by the caller's operation idempotency key. */
    Optional<JobControlIntent> findByRequestKey(String requestKey);

    /** Find all control intents still waiting for writer snapshot evidence. */
    List<JobControlIntent> findBySnapshotResultOrderByCreatedAtAsc(String snapshotResult);

    /**
     * Find writer lifecycle intents still waiting for target snapshot or source evidence.
     *
     * @param blockerType optional TARGET_SNAPSHOT or SOURCE_BLOCKED category
     * @param flowCode optional Flow whose node output uses this physical table
     * @param tableAssetKey optional normalized physical table
     * @param pageable bounded result page
     * @return newest matching control blockers first
     */
    @Query("""
            SELECT intent
            FROM JobControlIntent intent
            WHERE intent.snapshotResult = 'WAITING'
              AND (:tableAssetKey IS NULL OR intent.tableAssetKey = :tableAssetKey)
              AND (:flowCode IS NULL OR EXISTS (
                    SELECT node.id
                    FROM ScheduleNode node, FlowPlanVersion flowVersion
                    WHERE node.flowPlanVersionId = flowVersion.id
                      AND flowVersion.flowCode = :flowCode
                      AND (node.outputAssetKey = intent.tableAssetKey
                           OR node.outputAssetKey LIKE CONCAT(intent.tableAssetKey, '.%'))
              ))
              AND (:blockerType IS NULL
                   OR (:blockerType = 'TARGET_SNAPSHOT'
                       AND (intent.sourceHealth IS NULL OR intent.sourceHealth = 'HEALTHY'))
                   OR (:blockerType = 'SOURCE_BLOCKED'
                       AND intent.sourceHealth IN ('REPAIRABLE', 'SOURCE_BLOCKED')))
            ORDER BY intent.lastSnapshotCheckAt DESC NULLS LAST, intent.createdAt DESC, intent.id DESC
            """)
    List<JobControlIntent> findOperationalBlockers(
            @Param("blockerType") String blockerType,
            @Param("flowCode") String flowCode,
            @Param("tableAssetKey") String tableAssetKey,
            Pageable pageable);
}
