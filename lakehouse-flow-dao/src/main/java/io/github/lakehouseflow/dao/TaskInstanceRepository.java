package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.model.TaskInstance;
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
 * Repository for task scheduling instances and snapshot confirmation queries.
 */
@Repository
public interface TaskInstanceRepository extends JpaRepository<TaskInstance, Long> {

    /**
     * Find task instance by unique instance key
     */
    Optional<TaskInstance> findByInstanceKey(String instanceKey);

    /**
     * Lock one task intent while a downstream consumer claims it.
     *
     * @param id task scheduling instance id
     * @return locked task when present
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM TaskInstance t WHERE t.id = :id")
    Optional<TaskInstance> findByIdForUpdate(@Param("id") Long id);

    /**
     * Find all tasks in a workflow instance
     */
    List<TaskInstance> findByWorkflowInstanceIdOrderByCreatedAtAsc(Long workflowInstanceId);

    /**
     * Find all tasks in a specific state
     */
    List<TaskInstance> findByStateOrderByUpdatedAtDesc(String state);

    /**
     * Find all tasks in a state ordered for scheduling.
     */
    List<TaskInstance> findByStateOrderByCreatedAtAsc(String state);

    /**
     * Find ready task decisions whose parent workflow still allows publication.
     *
     * @return publishable task decisions in deterministic creation order
     */
    @Query("SELECT t FROM TaskInstance t " +
           "WHERE t.state = '" + SchedulingStates.READY_TO_SCHEDULE + "' " +
           "AND EXISTS (SELECT w.id FROM WorkflowInstance w " +
           "WHERE w.id = t.workflowInstanceId AND w.state <> '" + SchedulingStates.CANCELLED + "') " +
           "ORDER BY t.createdAt ASC")
    List<TaskInstance> findDeliverableReadyTasks();

    /**
     * Find tasks waiting for snapshot evidence.
     */
    @Query("SELECT t FROM TaskInstance t " +
           "WHERE t.state = '" + SchedulingStates.WAITING_SNAPSHOT + "' " +
           "ORDER BY t.createdAt ASC")
    List<TaskInstance> findWaitingForSnapshot();

    /**
     * Find scheduled tasks whose target snapshot should confirm the outcome.
     */
    @Query("SELECT t FROM TaskInstance t " +
           "WHERE t.state = '" + SchedulingStates.SCHEDULED + "' " +
           "AND t.targetAssetKey IS NOT NULL " +
           "ORDER BY t.scheduledAt ASC, t.createdAt ASC")
    List<TaskInstance> findScheduledAwaitingSnapshotConfirmation();

    /**
     * Find tasks in a workflow by task code
     */
    Optional<TaskInstance> findByWorkflowInstanceIdAndTaskCode(Long workflowInstanceId, String taskCode);

    /**
     * Find durable snapshot-confirmed tasks for one FlowPlan version and business date.
     *
     * The result is used only as evidence for an explicit whole-date backfill
     * skip policy. It is never used to release dependencies in a new workflow.
     *
     * @param flowPlanVersionId immutable definition version id
     * @param bizDate exact business date represented by the task
     * @param state required snapshot-confirmed scheduling state
     * @return newest confirmation evidence first
     */
    List<TaskInstance> findByFlowPlanVersionIdAndBizDateAndStateOrderByUpdatedAtDesc(
            Long flowPlanVersionId,
            LocalDateTime bizDate,
            String state);
}
