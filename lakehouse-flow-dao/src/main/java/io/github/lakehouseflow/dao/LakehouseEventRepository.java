package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.LakehouseEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface LakehouseEventRepository extends JpaRepository<LakehouseEvent, Long> {

    /**
     * Find event by unique event ID
     */
    Optional<LakehouseEvent> findByEventId(String eventId);

    /**
     * Find the durable snapshot-event sequence for one physical table.
     *
     * Intent attribution scans the sequence after its own baseline so a later
     * unrelated snapshot cannot hide an earlier matching commit.
     */
    @Query("SELECT e FROM LakehouseEvent e " +
           "WHERE e.catalogName = :catalogName " +
           "AND e.databaseName = :databaseName " +
           "AND e.tableName = :tableName " +
           "AND e.observedAt >= :observedAfter " +
           "ORDER BY e.observedAt ASC, e.id ASC")
    List<LakehouseEvent> findSnapshotEvidenceCandidates(
            @Param("catalogName") String catalogName,
            @Param("databaseName") String databaseName,
            @Param("tableName") String tableName,
            @Param("observedAfter") LocalDateTime observedAfter);

    /**
     * Find the most recently observed event for an asset.
     *
     * <p>Snapshot coordinates are stored as strings, so database lexical ordering would put
     * decimal coordinate {@code 99} after {@code 100}. Ingestion observation order is the durable
     * cross-format event order.
     */
    @Query("SELECT e FROM LakehouseEvent e " +
           "WHERE e.catalogName = :catalogName " +
           "AND e.databaseName = :databaseName " +
           "AND e.tableName = :tableName " +
           "AND (e.partitionName = :partitionName OR :partitionName IS NULL) " +
           "ORDER BY e.observedAt DESC, e.id DESC LIMIT 1")
    Optional<LakehouseEvent> findLatestEventByAsset(
            @Param("catalogName") String catalogName,
            @Param("databaseName") String databaseName,
            @Param("tableName") String tableName,
            @Param("partitionName") String partitionName
    );

    /**
     * Find the latest durable source event for one configured physical table.
     *
     * @param sourceType lakehouse format or source type
     * @param catalogName logical catalog name
     * @param databaseName logical database or schema name
     * @param tableName logical table name
     * @return latest matching event by ingestion order
     */
    @Query("SELECT e FROM LakehouseEvent e " +
           "WHERE e.sourceType = :sourceType " +
           "AND e.catalogName = :catalogName " +
           "AND e.databaseName = :databaseName " +
           "AND e.tableName = :tableName " +
           "ORDER BY e.observedAt DESC, e.id DESC LIMIT 1")
    Optional<LakehouseEvent> findLatestSourceEvent(
            @Param("sourceType") String sourceType,
            @Param("catalogName") String catalogName,
            @Param("databaseName") String databaseName,
            @Param("tableName") String tableName);

    /**
     * Find events since a specific time for a source
     */
    List<LakehouseEvent> findBySourceTypeAndObservedAtGreaterThanOrderByObservedAtAsc(
            String sourceType,
            LocalDateTime observedAfter
    );
}
