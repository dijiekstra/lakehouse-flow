package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.SchedulingTargetAdmission;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Repository for persistent target and business-date publication admission slots.
 */
@Repository
public interface SchedulingTargetAdmissionRepository
        extends JpaRepository<SchedulingTargetAdmission, Long> {

    /**
     * Create the reusable slot when this target and date have not been seen.
     *
     * PostgreSQL conflict handling serializes concurrent first acquisition
     * without exposing a uniqueness failure to the scheduling transaction.
     *
     * @param targetAssetKey normalized managed target asset key
     * @param bizDate protected business date
     */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            INSERT INTO scheduling_target_admission (
                target_asset_key,
                biz_date,
                status,
                version,
                created_at,
                updated_at
            ) VALUES (
                :targetAssetKey,
                :bizDate,
                'AVAILABLE',
                0,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (target_asset_key, biz_date) DO NOTHING
            """, nativeQuery = true)
    void ensureSlot(
            @Param("targetAssetKey") String targetAssetKey,
            @Param("bizDate") LocalDate bizDate);

    /**
     * Lock one admission slot while its holder is inspected or changed.
     *
     * @param targetAssetKey normalized managed target asset key
     * @param bizDate protected business date
     * @return locked slot when initialization succeeded
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM SchedulingTargetAdmission a "
            + "WHERE a.targetAssetKey = :targetAssetKey AND a.bizDate = :bizDate")
    Optional<SchedulingTargetAdmission> findForUpdate(
            @Param("targetAssetKey") String targetAssetKey,
            @Param("bizDate") LocalDate bizDate);
}
