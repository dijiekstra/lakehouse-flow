package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.SnapshotSourceHealth;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for durable table-level snapshot source health evidence.
 */
@Repository
public interface SnapshotSourceHealthRepository extends JpaRepository<SnapshotSourceHealth, Long> {

    /**
     * Find the unique managed source proof for a table asset.
     *
     * @param tableAssetKey normalized catalog.database.table key
     * @return latest persisted source health when managed
     */
    Optional<SnapshotSourceHealth> findByTableAssetKey(String tableAssetKey);

    /**
     * Find source health by the same identity used by durable ingestion offsets.
     *
     * @param sourceType lake format
     * @param sourceName configured source name
     * @return persisted source health when present
     */
    Optional<SnapshotSourceHealth> findBySourceTypeAndSourceName(String sourceType, String sourceName);

    /**
     * Query latest persisted reconciliation evidence with bounded optional filters.
     *
     * <p>A Flow filter matches sources used by any version of that Flow. Table targets also match
     * partition-qualified node outputs.
     *
     * @param sourceType normalized source type, or null
     * @param sourceName exact configured source name, or null
     * @param flowCode exact Flow code, or null
     * @param tableAssetKey normalized physical table key, or null
     * @param outcome persisted HEALTHY, REPAIRABLE, or BLOCKED outcome, or null
     * @param pageable bounded result page
     * @return latest matching source evidence first
     */
    @Query("""
            SELECT health
            FROM SnapshotSourceHealth health
            WHERE (:sourceType IS NULL OR health.sourceType = :sourceType)
              AND (:sourceName IS NULL OR health.sourceName = :sourceName)
              AND (:tableAssetKey IS NULL OR health.tableAssetKey = :tableAssetKey)
              AND (:outcome IS NULL OR health.outcome = :outcome)
              AND (:flowCode IS NULL OR EXISTS (
                    SELECT node.id
                    FROM ScheduleNode node, FlowPlanVersion flowVersion
                    WHERE node.flowPlanVersionId = flowVersion.id
                      AND flowVersion.flowCode = :flowCode
                      AND (node.outputAssetKey = health.tableAssetKey
                           OR node.outputAssetKey LIKE CONCAT(health.tableAssetKey, '.%'))
              ))
            ORDER BY health.evidenceCheckedAt DESC, health.id DESC
            """)
    List<SnapshotSourceHealth> findLatestEvidence(
            @Param("sourceType") String sourceType,
            @Param("sourceName") String sourceName,
            @Param("flowCode") String flowCode,
            @Param("tableAssetKey") String tableAssetKey,
            @Param("outcome") String outcome,
            Pageable pageable);
}
