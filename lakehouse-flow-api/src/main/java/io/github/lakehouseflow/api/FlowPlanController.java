package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.CreateFlowPlanRequest;
import io.github.lakehouseflow.api.dto.CreateFlowPlanVersionRequest;
import io.github.lakehouseflow.api.dto.CreateScheduleNodeRequest;
import io.github.lakehouseflow.api.dto.FlowPlanResponse;
import io.github.lakehouseflow.api.dto.FlowPlanVersionResponse;
import io.github.lakehouseflow.api.dto.PublishFlowPlanVersionRequest;
import io.github.lakehouseflow.api.dto.ScheduleNodeResponse;
import io.github.lakehouseflow.model.FlowPlan;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.service.FlowPlanService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST API for managing scheduler-side FlowPlan definitions.
 *
 * These endpoints expose FlowPlan and node metadata only. Publishing a version
 * makes it eligible for future scheduling decisions but does not execute work.
 */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class FlowPlanController {

    private final FlowPlanService flowPlanService;

    /**
     * Create or return a FlowPlan draft by code.
     *
     * @param request FlowPlan creation request
     * @return created or existing FlowPlan
     */
    @PostMapping("/flow-plans")
    public FlowPlanResponse createDraftPlan(@Valid @RequestBody CreateFlowPlanRequest request) {
        FlowPlan flowPlan = flowPlanService.createDraftPlan(new FlowPlanService.CreateFlowPlanCommand(
                request.flowCode(),
                request.flowName(),
                request.flowSpaceCode(),
                request.owner(),
                request.description()));
        return toResponse(flowPlan);
    }

    /**
     * Create or return a draft version for a FlowPlan.
     *
     * @param flowPlanId owning FlowPlan id
     * @param request FlowPlan version creation request
     * @return created or existing FlowPlan version
     */
    @PostMapping("/flow-plans/{flowPlanId}/versions")
    public FlowPlanVersionResponse createDraftVersion(
            @PathVariable Long flowPlanId,
            @RequestBody CreateFlowPlanVersionRequest request) {

        FlowPlanVersion version = flowPlanService.createDraftVersion(new FlowPlanService.CreateFlowPlanVersionCommand(
                flowPlanId,
                request.version(),
                request.graphJson(),
                request.dependencySpecJson(),
                request.triggerPolicyJson(),
                request.confirmationPolicyJson(),
                request.concurrencyPolicyJson()));
        return toResponse(version);
    }

    /**
     * Add or return a node in a FlowPlan version draft.
     *
     * @param flowPlanVersionId owning FlowPlanVersion id
     * @param request schedule node creation request
     * @return created or existing schedule node
     */
    @PostMapping("/flow-plan-versions/{flowPlanVersionId}/nodes")
    public ScheduleNodeResponse addNode(
            @PathVariable Long flowPlanVersionId,
            @Valid @RequestBody CreateScheduleNodeRequest request) {

        ScheduleNode node = flowPlanService.addNode(new FlowPlanService.CreateScheduleNodeCommand(
                flowPlanVersionId,
                request.nodeCode(),
                request.nodeName(),
                request.nodeType(),
                request.dependsOnNodes(),
                request.inputDependencySpecJson(),
                request.outputAssetKey(),
                request.confirmationPolicyJson(),
                request.sortOrder()));
        return toResponse(node);
    }

    /**
     * Publish a FlowPlan version for future scheduling decisions.
     *
     * @param flowPlanVersionId FlowPlanVersion id
     * @param request publication request
     * @return published FlowPlan version
     */
    @PostMapping("/flow-plan-versions/{flowPlanVersionId}/publish")
    public FlowPlanVersionResponse publishVersion(
            @PathVariable Long flowPlanVersionId,
            @RequestBody PublishFlowPlanVersionRequest request) {

        return toResponse(flowPlanService.publishVersion(flowPlanVersionId, request.publishedBy()));
    }

    /**
     * List nodes for a FlowPlan version.
     *
     * @param flowPlanVersionId FlowPlanVersion id
     * @return ordered schedule nodes
     */
    @GetMapping("/flow-plan-versions/{flowPlanVersionId}/nodes")
    public List<ScheduleNodeResponse> listVersionNodes(@PathVariable Long flowPlanVersionId) {
        return flowPlanService.findVersionNodes(flowPlanVersionId).stream()
                .map(this::toResponse)
                .toList();
    }

    /**
     * Read the latest published version for a FlowPlan.
     *
     * @param flowPlanId FlowPlan id
     * @return latest published version or 404 when none exists
     */
    @GetMapping("/flow-plans/{flowPlanId}/versions/latest-published")
    public ResponseEntity<FlowPlanVersionResponse> getLatestPublishedVersion(@PathVariable Long flowPlanId) {
        return flowPlanService.findLatestPublishedVersion(flowPlanId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Convert a FlowPlan entity into an API response.
     *
     * @param flowPlan FlowPlan entity
     * @return API response
     */
    private FlowPlanResponse toResponse(FlowPlan flowPlan) {
        return new FlowPlanResponse(
                flowPlan.getId(),
                flowPlan.getFlowCode(),
                flowPlan.getFlowName(),
                flowPlan.getFlowSpaceCode(),
                flowPlan.getOwner(),
                flowPlan.getDescription(),
                flowPlan.getStatus(),
                flowPlan.getCurrentVersion(),
                flowPlan.getCreatedAt(),
                flowPlan.getUpdatedAt());
    }

    /**
     * Convert a FlowPlanVersion entity into an API response.
     *
     * @param version FlowPlanVersion entity
     * @return API response
     */
    private FlowPlanVersionResponse toResponse(FlowPlanVersion version) {
        return new FlowPlanVersionResponse(
                version.getId(),
                version.getFlowPlanId(),
                version.getFlowCode(),
                version.getVersion(),
                version.getStatus(),
                version.getGraphJson(),
                version.getDependencySpecJson(),
                version.getTriggerPolicyJson(),
                version.getConfirmationPolicyJson(),
                version.getConcurrencyPolicyJson(),
                version.getPublishedBy(),
                version.getPublishedAt(),
                version.getCreatedAt(),
                version.getUpdatedAt());
    }

    /**
     * Convert a ScheduleNode entity into an API response.
     *
     * @param node ScheduleNode entity
     * @return API response
     */
    private ScheduleNodeResponse toResponse(ScheduleNode node) {
        return new ScheduleNodeResponse(
                node.getId(),
                node.getFlowPlanVersionId(),
                node.getNodeCode(),
                node.getNodeName(),
                node.getNodeType(),
                node.getDependsOnNodes(),
                node.getInputDependencySpecJson(),
                node.getOutputAssetKey(),
                node.getConfirmationPolicyJson(),
                node.getSortOrder(),
                node.getCreatedAt(),
                node.getUpdatedAt());
    }
}
