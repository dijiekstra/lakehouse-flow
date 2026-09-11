package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.TaskInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface TaskInstanceRepository extends JpaRepository<TaskInstance, Long> {

    /**
     * Find task instance by unique instance key
     */
    Optional<TaskInstance> findByInstanceKey(String instanceKey);

    /**
     * Find all tasks in a workflow instance
     */
    List<TaskInstance> findByWorkflowInstanceIdOrderByCreatedAtAsc(Long workflowInstanceId);

    /**
     * Find all tasks in a specific state
     */
    List<TaskInstance> findByStateOrderByUpdatedAtDesc(String state);

    /**
     * Find all tasks ready to dispatch
     */
    List<TaskInstance> findByStateOrderByCreatedAtAsc(String state);

    /**
     * Find tasks waiting for dependencies
     */
    @Query("SELECT t FROM TaskInstance t " +
           "WHERE t.state = 'WAITING_DEPENDENCY' " +
           "ORDER BY t.createdAt ASC")
    List<TaskInstance> findWaitingForDependencies();

    /**
     * Find tasks that are stuck in RUNNING state
     */
    @Query("SELECT t FROM TaskInstance t " +
           "WHERE t.state = 'RUNNING' " +
           "AND t.updatedAt < :beforeTime " +
           "ORDER BY t.updatedAt ASC")
    List<TaskInstance> findStuckRunningTasks(@Param("beforeTime") LocalDateTime beforeTime);

    /**
     * Find tasks by external job ID
     */
    Optional<TaskInstance> findByExternalJobId(String externalJobId);

    /**
     * Find tasks in a workflow by task code
     */
    Optional<TaskInstance> findByWorkflowInstanceIdAndTaskCode(Long workflowInstanceId, String taskCode);
}
