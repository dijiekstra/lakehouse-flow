package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.FlowPlanVersion;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for versioned FlowPlan definitions.
 */
@Repository
public interface FlowPlanVersionRepository extends JpaRepository<FlowPlanVersion, Long> {

    /**
     * Lock a version while admitting its first workflow intent.
     *
     * @param id FlowPlanVersion id
     * @return locked version when present
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT v FROM FlowPlanVersion v WHERE v.id = :id")
    Optional<FlowPlanVersion> findByIdForUpdate(@Param("id") Long id);

    /**
     * Find a specific version within a FlowPlan.
     *
     * @param flowPlanId owning FlowPlan id
     * @param version version number
     * @return matching version when it exists
     */
    Optional<FlowPlanVersion> findByFlowPlanIdAndVersion(Long flowPlanId, Integer version);

    /**
     * Find all versions for a FlowPlan.
     *
     * @param flowPlanId owning FlowPlan id
     * @return newest versions first
     */
    List<FlowPlanVersion> findByFlowPlanIdOrderByVersionDesc(Long flowPlanId);

    /**
     * Find the newest version in a lifecycle status for a FlowPlan.
     *
     * @param flowPlanId owning FlowPlan id
     * @param status version lifecycle status
     * @return newest matching version when it exists
     */
    Optional<FlowPlanVersion> findFirstByFlowPlanIdAndStatusOrderByVersionDesc(Long flowPlanId, String status);

    /**
     * Find a version by denormalized Flow code and version number.
     *
     * @param flowCode FlowPlan code
     * @param version version number
     * @return matching version when it exists
     */
    Optional<FlowPlanVersion> findByFlowCodeAndVersion(String flowCode, Integer version);

    /**
     * Find all versions eligible for snapshot-driven scheduling evaluation.
     *
     * @param status lifecycle status to scan
     * @return matching versions in deterministic update order
     */
    List<FlowPlanVersion> findByStatusOrderByUpdatedAtAsc(String status);
}
