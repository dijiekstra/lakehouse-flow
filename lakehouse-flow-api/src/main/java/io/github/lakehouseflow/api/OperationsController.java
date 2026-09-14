package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.OperationalBlockerResponse;
import io.github.lakehouseflow.api.dto.SnapshotSourceEvidenceResponse;
import io.github.lakehouseflow.service.SchedulingBlockerQueryService;
import io.github.lakehouseflow.service.SnapshotSourceHealthQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Minimal read-only operations API for blockers and persisted source evidence.
 */
@RestController
@RequestMapping("/api/v1/operations")
@RequiredArgsConstructor
public class OperationsController {

    private final SchedulingBlockerQueryService blockerQueryService;
    private final SnapshotSourceHealthQueryService sourceHealthQueryService;

    /**
     * List current scheduler, delivery, and snapshot-evidence blockers.
     *
     * @param blockerType optional stable blocker category
     * @param flowCode optional owning or affected Flow
     * @param targetAssetKey optional exact target or physical-table prefix
     * @param limit bounded row count
     * @return newest matching blockers first
     */
    @GetMapping("/blockers")
    public List<OperationalBlockerResponse> findBlockers(
            @RequestParam(required = false) String blockerType,
            @RequestParam(required = false) String flowCode,
            @RequestParam(required = false) String targetAssetKey,
            @RequestParam(required = false) Integer limit) {
        return blockerQueryService.findBlockers(blockerType, flowCode, targetAssetKey, limit).stream()
                .map(blocker -> new OperationalBlockerResponse(
                        blocker.blockerType(),
                        blocker.subjectType(),
                        blocker.subjectId(),
                        blocker.subjectKey(),
                        blocker.flowCode(),
                        blocker.taskCode(),
                        blocker.writerJobKey(),
                        blocker.targetAssetKey(),
                        blocker.bizDate(),
                        blocker.schedulingState(),
                        blocker.deliveryStatus(),
                        blocker.sourceHealth(),
                        blocker.reason(),
                        blocker.observedAt()))
                .toList();
    }

    /**
     * List latest persisted snapshot-source reconciliation evidence.
     *
     * @param sourceType optional lake-format source type
     * @param sourceName optional configured source name
     * @param flowCode optional Flow using the managed output table
     * @param targetAssetKey optional table or partition target
     * @param sourceHealth optional HEALTHY, REPAIRABLE, or SOURCE_BLOCKED result
     * @param limit bounded row count
     * @return latest matching source evidence first
     */
    @GetMapping("/snapshot-sources")
    public List<SnapshotSourceEvidenceResponse> findSnapshotSources(
            @RequestParam(required = false) String sourceType,
            @RequestParam(required = false) String sourceName,
            @RequestParam(required = false) String flowCode,
            @RequestParam(required = false) String targetAssetKey,
            @RequestParam(required = false) String sourceHealth,
            @RequestParam(required = false) Integer limit) {
        return sourceHealthQueryService.findLatestEvidence(
                        sourceType, sourceName, flowCode, targetAssetKey, sourceHealth, limit)
                .stream()
                .map(evidence -> new SnapshotSourceEvidenceResponse(
                        evidence.id(),
                        evidence.sourceType(),
                        evidence.sourceName(),
                        evidence.tableAssetKey(),
                        evidence.sourceHealth(),
                        evidence.offsetStatus(),
                        evidence.projectionStatus(),
                        evidence.durableOffset(),
                        evidence.latestSourceOffset(),
                        evidence.evidenceCheckedAt(),
                        evidence.detail(),
                        evidence.updatedAt()))
                .toList();
    }
}
