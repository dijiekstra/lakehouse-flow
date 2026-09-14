package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.model.TaskInstance;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
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
     * Count non-terminal task scheduling instances by their stable scheduler state.
     *
     * @return one grouped count for each currently present non-terminal state
     */
    @Query("SELECT t.state AS state, COUNT(t.id) AS instanceCount " +
           "FROM TaskInstance t " +
           "WHERE t.state IN ('" + SchedulingStates.CREATED + "', '" +
           SchedulingStates.WAITING_SNAPSHOT + "', '" +
           SchedulingStates.READY_TO_SCHEDULE + "', '" +
           SchedulingStates.SCHEDULED + "') " +
           "GROUP BY t.state")
    List<TaskInstanceStateCount> countNonTerminalByState();

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

    /**
     * Find task scheduling records that currently occupy an operational wait phase.
     *
     * @param blockerType optional stable blocker category
     * @param flowCode optional owning workflow or Flow code
     * @param targetAssetKey optional exact table/partition target or table prefix
     * @param pageable bounded result page
     * @return newest matching scheduler-owned blockers first
     */
    @Query("""
            SELECT task
            FROM TaskInstance task, WorkflowInstance workflow
            WHERE workflow.id = task.workflowInstanceId
              AND task.state IN ('WAITING_SNAPSHOT', 'READY_TO_SCHEDULE', 'SCHEDULED')
              AND (:flowCode IS NULL OR workflow.workflowCode = :flowCode)
              AND (:targetAssetKey IS NULL
                   OR task.targetAssetKey = :targetAssetKey
                   OR task.targetAssetKey LIKE CONCAT(:targetAssetKey, '.%'))
              AND (:blockerType IS NULL
                   OR (:blockerType = 'INPUT_SNAPSHOT' AND task.state = 'WAITING_SNAPSHOT')
                   OR (:blockerType = 'INTENT_PUBLICATION' AND task.state = 'READY_TO_SCHEDULE')
                   OR (:blockerType = 'TARGET_SNAPSHOT' AND task.state = 'SCHEDULED'
                       AND (task.sourceHealth IS NULL OR task.sourceHealth = 'HEALTHY'))
                   OR (:blockerType = 'SOURCE_BLOCKED' AND task.state = 'SCHEDULED'
                       AND task.sourceHealth IN ('REPAIRABLE', 'SOURCE_BLOCKED')))
            ORDER BY task.updatedAt DESC, task.id DESC
            """)
    List<TaskInstance> findOperationalBlockers(
            @Param("blockerType") String blockerType,
            @Param("flowCode") String flowCode,
            @Param("targetAssetKey") String targetAssetKey,
            Pageable pageable);
}
