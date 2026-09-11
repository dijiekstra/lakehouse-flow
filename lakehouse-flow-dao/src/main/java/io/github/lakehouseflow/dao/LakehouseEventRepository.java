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
     * Find all events for an asset within a snapshot range
     */
    @Query("SELECT e FROM LakehouseEvent e " +
           "WHERE e.catalogName = :catalogName " +
           "AND e.databaseName = :databaseName " +
           "AND e.tableName = :tableName " +
           "AND (e.partitionName = :partitionName OR :partitionName IS NULL) " +
           "AND e.snapshotId >= :minSnapshotId " +
           "ORDER BY e.snapshotId ASC")
    List<LakehouseEvent> findEventsByAssetAndSnapshotRange(
            @Param("catalogName") String catalogName,
            @Param("databaseName") String databaseName,
            @Param("tableName") String tableName,
            @Param("partitionName") String partitionName,
            @Param("minSnapshotId") String minSnapshotId
    );

    /**
     * Find latest events for an asset
     */
    @Query("SELECT e FROM LakehouseEvent e " +
           "WHERE e.catalogName = :catalogName " +
           "AND e.databaseName = :databaseName " +
           "AND e.tableName = :tableName " +
           "AND (e.partitionName = :partitionName OR :partitionName IS NULL) " +
           "ORDER BY e.snapshotId DESC LIMIT 1")
    Optional<LakehouseEvent> findLatestEventByAsset(
            @Param("catalogName") String catalogName,
            @Param("databaseName") String databaseName,
            @Param("tableName") String tableName,
            @Param("partitionName") String partitionName
    );

    /**
     * Find events since a specific time for a source
     */
    List<LakehouseEvent> findBySourceTypeAndObservedAtGreaterThanOrderByObservedAtAsc(
            String sourceType,
            LocalDateTime observedAfter
    );
}
