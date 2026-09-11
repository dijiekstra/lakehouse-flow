package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.EventConsumerOffset;
import org.springframework.data.jpa.repository.JpaRepository;
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
}
