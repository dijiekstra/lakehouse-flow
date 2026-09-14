package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.SnapshotSourceHealth;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

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
}
