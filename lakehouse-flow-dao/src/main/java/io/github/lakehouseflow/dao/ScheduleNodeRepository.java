package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.ScheduleNode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for ScheduleNode definitions inside FlowPlan versions.
 */
@Repository
public interface ScheduleNodeRepository extends JpaRepository<ScheduleNode, Long> {

    /**
     * Find nodes for a FlowPlan version in deterministic graph order.
     *
     * @param flowPlanVersionId FlowPlanVersion id
     * @return ordered schedule nodes
     */
    List<ScheduleNode> findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(Long flowPlanVersionId);

    /**
     * Find a node by code within a FlowPlan version.
     *
     * @param flowPlanVersionId FlowPlanVersion id
     * @param nodeCode node code unique within the version
     * @return matching node when it exists
     */
    Optional<ScheduleNode> findByFlowPlanVersionIdAndNodeCode(Long flowPlanVersionId, String nodeCode);

    /**
     * Find nodes that confirm a specific output asset.
     *
     * @param outputAssetKey target asset key
     * @return matching nodes
     */
    List<ScheduleNode> findByOutputAssetKey(String outputAssetKey);

}
