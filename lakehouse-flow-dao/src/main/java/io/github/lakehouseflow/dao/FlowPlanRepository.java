package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.FlowPlan;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for scheduler-side FlowPlan definitions.
 */
@Repository
public interface FlowPlanRepository extends JpaRepository<FlowPlan, Long> {

    /**
     * Find a FlowPlan by its stable code.
     *
     * @param flowCode FlowPlan code
     * @return matching FlowPlan when it exists
     */
    Optional<FlowPlan> findByFlowCode(String flowCode);

    /**
     * Check whether a FlowPlan code is already registered.
     *
     * @param flowCode FlowPlan code
     * @return true when the code exists
     */
    boolean existsByFlowCode(String flowCode);

    /**
     * Find FlowPlans by lifecycle status for management views.
     *
     * @param status FlowPlan lifecycle status
     * @return newest matching FlowPlans first
     */
    List<FlowPlan> findByStatusOrderByUpdatedAtDesc(String status);

    /**
     * Find FlowPlans within a lightweight sharing space.
     *
     * @param flowSpaceCode Flow space code
     * @return newest matching FlowPlans first
     */
    List<FlowPlan> findByFlowSpaceCodeOrderByUpdatedAtDesc(String flowSpaceCode);
}
