package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.EventConsumerOffset;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * EventConsumerOffset Repository - Tracks last processed offset for each event source.
 */
@Repository
public interface EventConsumerOffsetRepository extends JpaRepository<EventConsumerOffset, Long> {

    /**
     * Find offset by source type and source name.
     *
     * @param sourceType Event source type (e.g., "PAIMON")
     * @param sourceName Event source instance name (e.g., "paimon_catalog.ods.orders")
     * @return Optional containing the offset, or empty if not found
     */
    Optional<EventConsumerOffset> findBySourceTypeAndSourceName(String sourceType, String sourceName);

    /**
     * Lock an existing source offset while a projected snapshot advances it.
     *
     * @param sourceType event source type
     * @param sourceName stable source instance name
     * @return locked offset when the source was already initialized
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM EventConsumerOffset o "
            + "WHERE o.sourceType = :sourceType AND o.sourceName = :sourceName")
    Optional<EventConsumerOffset> findForUpdate(
            @Param("sourceType") String sourceType,
            @Param("sourceName") String sourceName);
}
