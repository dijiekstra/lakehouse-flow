package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.TriggerHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for TriggerHistory (audit log).
 *
 * Stores records of why workflow and task instances were created.
 */
@Repository
public interface TriggerHistoryRepository extends JpaRepository<TriggerHistory, Long> {

    /**
     * Find trigger history by unique trigger key.
     * Used for idempotency checks.
     */
    Optional<TriggerHistory> findByTriggerKey(String triggerKey);

    /**
     * Find all triggers for a workflow instance.
     * Used for debugging why this workflow was created.
     */
    List<TriggerHistory> findByWorkflowInstanceId(Long workflowInstanceId);

    /**
     * Find all triggers for a task instance.
     * Used for debugging why this task was created.
     */
    List<TriggerHistory> findByTaskInstanceId(Long taskInstanceId);

    /**
     * Find all triggers that were TRIGGERED (not SKIPPED).
     */
    List<TriggerHistory> findByDecision(String decision);

    /**
     * Find all triggers for an asset.
     * Used for tracing what snapshots triggered what workflows.
     */
    List<TriggerHistory> findByAssetKeyOrderByCreatedAtDesc(String assetKey);

    /**
     * Find all triggers for a specific snapshot.
     * Used for tracing what workflows were created from this snapshot.
     */
    List<TriggerHistory> findBySnapshotIdOrderByCreatedAtDesc(String snapshotId);

    /**
     * Find all triggers for a specific trigger type.
     * Used for statistics (e.g., how many SNAPSHOT_DRIVEN vs MANUAL triggers).
     */
    List<TriggerHistory> findByTriggerTypeOrderByCreatedAtDesc(String triggerType);
}
