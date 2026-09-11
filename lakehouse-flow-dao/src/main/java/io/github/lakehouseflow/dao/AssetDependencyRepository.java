package io.github.lakehouseflow.dao;

import io.github.lakehouseflow.model.AssetDependency;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AssetDependencyRepository extends JpaRepository<AssetDependency, Long> {

    /**
     * Find all dependencies for an asset
     */
    List<AssetDependency> findByAssetKeyAndEnabledTrueOrderByCreatedAtAsc(String assetKey);

    /**
     * Find all dependencies for a workflow
     */
    List<AssetDependency> findByWorkflowCodeAndEnabledTrueOrderByCreatedAtAsc(String workflowCode);

    /**
     * Find all dependencies for a task
     */
    List<AssetDependency> findByTaskCodeAndEnabledTrueOrderByCreatedAtAsc(String taskCode);
}
