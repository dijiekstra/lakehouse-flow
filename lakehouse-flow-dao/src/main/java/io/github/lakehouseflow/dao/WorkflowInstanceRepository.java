package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.WorkflowInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstance, Long> {

    /**
     * Find instance by unique instance key
     */
    Optional<WorkflowInstance> findByInstanceKey(String instanceKey);

    /**
     * Find all instances for a workflow code
     */
    List<WorkflowInstance> findByWorkflowCodeOrderByCreatedAtDesc(String workflowCode);

    /**
     * Find all instances in a specific state
     */
    List<WorkflowInstance> findByStateOrderByUpdatedAtDesc(String state);

    /**
     * Find all instances waiting for resources
     */
    @Query("SELECT w FROM WorkflowInstance w WHERE w.state IN ('CREATED', 'WAITING') ORDER BY w.createdAt ASC")
    List<WorkflowInstance> findWaitingInstances();

    /**
     * Find instances by state and time window
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
