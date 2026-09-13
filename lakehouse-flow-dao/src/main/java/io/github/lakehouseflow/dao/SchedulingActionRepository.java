package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.SchedulingAction;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for durable scheduling-side action requests.
 */
@Repository
public interface SchedulingActionRepository extends JpaRepository<SchedulingAction, Long> {

    /**
     * Find an action by its caller-provided idempotency key.
     *
     * @param actionKey idempotency key
     * @return matching action when already recorded
     */
    Optional<SchedulingAction> findByActionKey(String actionKey);

    /**
     * Search recent actions by immutable FlowPlan definition anchors.
     *
     * Null parameters are ignored so callers can combine workflow, version,
     * and node filters without constructing dynamic query strings.
     *
     * @param workflowCode workflow code filter, or null
     * @param flowPlanVersionId published FlowPlanVersion id filter, or null
     * @param scheduleNodeId ScheduleNode id filter, or null
     * @param pageable result limit and paging request
     * @return newest matching actions first
     */
    @Query("""
            SELECT action
            FROM SchedulingAction action
            WHERE (:workflowCode IS NULL OR action.workflowCode = :workflowCode)
              AND (:flowPlanVersionId IS NULL OR action.flowPlanVersionId = :flowPlanVersionId)
              AND (:scheduleNodeId IS NULL OR action.scheduleNodeId = :scheduleNodeId)
            ORDER BY action.createdAt DESC, action.id DESC
            """)
    List<SchedulingAction> searchByDefinitionAnchors(
            @Param("workflowCode") String workflowCode,
            @Param("flowPlanVersionId") Long flowPlanVersionId,
            @Param("scheduleNodeId") Long scheduleNodeId,
            Pageable pageable);

    /**
     * Find actions of a specific type for audit views.
     *
     * @param actionType scheduling action type
     * @return newest actions first
     */
    List<SchedulingAction> findByActionTypeOrderByCreatedAtDesc(String actionType);

    /**
     * Find actions targeting a workflow instance.
     *
     * @param workflowInstanceId workflow instance id
     * @return newest matching actions first
     */
    List<SchedulingAction> findByWorkflowInstanceIdOrderByCreatedAtDesc(Long workflowInstanceId);

    /**
     * Find actions targeting a task instance.
     *
     * @param taskInstanceId task instance id
     * @return newest matching actions first
     */
    List<SchedulingAction> findByTaskInstanceIdOrderByCreatedAtDesc(Long taskInstanceId);

    /**
     * Find actions by processing status.
     *
     * @param status action processing status
     * @return oldest matching actions first
     */
    List<SchedulingAction> findByStatusOrderByCreatedAtAsc(String status);
}
