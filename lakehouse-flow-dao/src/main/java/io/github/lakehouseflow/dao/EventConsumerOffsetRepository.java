package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.EventConsumerOffset;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
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
     * Create the source coordination row before locking it for ingestion.
     *
     * <p>PostgreSQL conflict handling makes concurrent first observation safe. The inserted
     * candidate offset remains part of the caller's transaction, so a later projection failure
     * rolls it back together with the event and AssetState changes.
     *
     * @param sourceType event source type
     * @param sourceName stable source instance name
     * @param sourceOffset first candidate offset
     */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            INSERT INTO event_consumer_offset (
                source_type,
                source_name,
                offset_value,
                created_at,
                updated_at
            ) VALUES (
                :sourceType,
                :sourceName,
                :sourceOffset,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (source_type, source_name) DO NOTHING
            """, nativeQuery = true)
    void ensureOffset(
            @Param("sourceType") String sourceType,
            @Param("sourceName") String sourceName,
            @Param("sourceOffset") String sourceOffset);

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
