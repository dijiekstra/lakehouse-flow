package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.AssetState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AssetStateRepository extends JpaRepository<AssetState, Long> {

    /**
     * Find asset state by unique asset key
     */
    Optional<AssetState> findByAssetKey(String assetKey);

    /**
     * Find all assets that are ready
     */
    List<AssetState> findByReadinessStatusOrderByUpdatedAtDesc(String readinessStatus);

    /**
     * Find all assets for a table
     */
    @Query("SELECT a FROM AssetState a " +
           "WHERE a.catalogName = :catalogName " +
           "AND a.databaseName = :databaseName " +
           "AND a.tableName = :tableName " +
           "ORDER BY a.assetKey")
    List<AssetState> findByTable(
            @Param("catalogName") String catalogName,
            @Param("databaseName") String databaseName,
            @Param("tableName") String tableName
    );

    /**
     * Find all assets with a specific quality status
     */
    List<AssetState> findByQualityStatusOrderByUpdatedAtDesc(String qualityStatus);
}
