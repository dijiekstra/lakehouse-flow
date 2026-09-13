package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.model.WorkflowInstance;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for workflow scheduling instances.
 */
@Repository
public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstance, Long> {

    /**
     * Find instance by unique instance key
     */
    Optional<WorkflowInstance> findByInstanceKey(String instanceKey);

    /**
     * Lock one workflow while cancelling it or claiming one of its task intents.
     *
     * @param id workflow scheduling instance id
     * @return locked workflow when present
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM WorkflowInstance w WHERE w.id = :id")
    Optional<WorkflowInstance> findByIdForUpdate(@Param("id") Long id);

    /**
     * Find all instances for a workflow code
     */
    List<WorkflowInstance> findByWorkflowCodeOrderByCreatedAtDesc(String workflowCode);

    /**
     * Find all instances in a specific state
     */
    List<WorkflowInstance> findByStateOrderByUpdatedAtDesc(String state);

    /**
     * Count workflow scheduling instances occupying a version concurrency slot.
     *
     * @param flowPlanVersionId immutable FlowPlanVersion id
     * @param state active scheduling state
     * @return matching active instance count
     */
    long countByFlowPlanVersionIdAndState(Long flowPlanVersionId, String state);

    /**
     * Find all instances not yet scheduled.
     */
    @Query("SELECT w FROM WorkflowInstance w " +
           "WHERE w.state IN ('" + SchedulingStates.CREATED + "', '" +
           SchedulingStates.WAITING_SNAPSHOT + "', '" +
           SchedulingStates.READY_TO_SCHEDULE + "') " +
           "ORDER BY w.createdAt ASC")
    List<WorkflowInstance> findWaitingInstances();

    /**
     * Find instances by scheduling state and time window.
     */
    @Query("SELECT w FROM WorkflowInstance w " +
           "WHERE w.state = :state " +
           "AND w.updatedAt < :beforeTime " +
           "ORDER BY w.updatedAt ASC")
    List<WorkflowInstance> findStuckInstances(
            @Param("state") String state,
            @Param("beforeTime") LocalDateTime beforeTime
    );
}
