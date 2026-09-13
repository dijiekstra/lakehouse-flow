package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.FlowPlanStatuses;
import io.github.lakehouseflow.common.FlowPlanVersionStatuses;
import io.github.lakehouseflow.common.ScheduleNodeTypes;
import io.github.lakehouseflow.dao.FlowPlanRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.model.FlowPlan;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service for managing scheduler-side FlowPlan definitions.
 *
 * This service only manages scheduling definitions and publication state. It
 * does not execute downstream work; published versions are used later as
 * anchors for scheduling decisions and snapshot confirmation.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class FlowPlanService {

    private final FlowPlanRepository flowPlanRepository;
    private final FlowPlanVersionRepository flowPlanVersionRepository;
    private final ScheduleNodeRepository scheduleNodeRepository;
    private final FlowPlanGraphService flowPlanGraphService;
    private final FlowPlanPolicyService flowPlanPolicyService;

    /**
     * Command for creating a FlowPlan draft.
     *
     * @param flowCode stable Flow code
     * @param flowName human-readable Flow name
     * @param flowSpaceCode lightweight sharing boundary
     * @param owner Flow owner
     * @param description human-readable description
     */
    public record CreateFlowPlanCommand(
            String flowCode,
            String flowName,
            String flowSpaceCode,
            String owner,
            String description) {
    }

    /**
     * Command for creating a FlowPlanVersion draft.
     *
     * @param flowPlanId owning FlowPlan id
     * @param version requested version, or null to use currentVersion + 1
     * @param graphJson graph metadata
     * @param dependencySpecJson Flow-level dependency defaults
     * @param triggerPolicyJson trigger policy
     * @param confirmationPolicyJson snapshot confirmation policy
     * @param concurrencyPolicyJson scheduler-side concurrency policy
     */
    public record CreateFlowPlanVersionCommand(
            Long flowPlanId,
            Integer version,
            Map<String, Object> graphJson,
            Map<String, Object> dependencySpecJson,
            Map<String, Object> triggerPolicyJson,
            Map<String, Object> confirmationPolicyJson,
            Map<String, Object> concurrencyPolicyJson) {
    }

    /**
     * Command for adding a node to a FlowPlanVersion draft.
     *
     * @param flowPlanVersionId owning FlowPlanVersion id
     * @param nodeCode stable node code within the version
     * @param nodeName human-readable node name
     * @param nodeType scheduler-side node type
     * @param dependsOnNodes upstream node codes
     * @param inputDependencySpecJson input asset dependency conditions
     * @param outputAssetKey target asset expected to advance after scheduling
     * @param confirmationPolicyJson node-level snapshot confirmation policy
     * @param sortOrder deterministic graph ordering hint
     */
    public record CreateScheduleNodeCommand(
            Long flowPlanVersionId,
            String nodeCode,
            String nodeName,
            String nodeType,
            List<String> dependsOnNodes,
            Map<String, Object> inputDependencySpecJson,
            String outputAssetKey,
            Map<String, Object> confirmationPolicyJson,
            Integer sortOrder) {
    }

    /**
     * Create a FlowPlan draft or return the existing FlowPlan with the same code.
     *
     * @param command FlowPlan creation command
     * @return newly created or existing FlowPlan
     */
    public FlowPlan createDraftPlan(CreateFlowPlanCommand command) {
        String flowCode = requireText(command.flowCode(), "flowCode");
        Optional<FlowPlan> existing = flowPlanRepository.findByFlowCode(flowCode);
        if (existing.isPresent()) {
            log.debug("FlowPlan {} already exists, returning existing definition", flowCode);
            return existing.get();
        }

        FlowPlan flowPlan = FlowPlan.builder()
                .flowCode(flowCode)
                .flowName(requireText(command.flowName(), "flowName"))
                .flowSpaceCode(blankToNull(command.flowSpaceCode()))
                .owner(blankToNull(command.owner()))
                .description(command.description())
                .status(FlowPlanStatuses.DRAFT)
                .currentVersion(0)
                .build();

        return flowPlanRepository.save(flowPlan);
    }

    /**
     * Create a draft version for a FlowPlan or return the existing matching version.
     *
     * @param command version creation command
     * @return newly created or existing FlowPlanVersion
     */
    public FlowPlanVersion createDraftVersion(CreateFlowPlanVersionCommand command) {
        FlowPlan flowPlan = getFlowPlanOrThrow(command.flowPlanId());
        Integer version = normalizeVersion(command.version(), flowPlan);

        Optional<FlowPlanVersion> existing = flowPlanVersionRepository
                .findByFlowPlanIdAndVersion(flowPlan.getId(), version);
        if (existing.isPresent()) {
            log.debug("FlowPlan {} version {} already exists, returning existing version",
                    flowPlan.getFlowCode(), version);
            return existing.get();
        }

        FlowPlanVersion flowPlanVersion = FlowPlanVersion.builder()
                .flowPlanId(flowPlan.getId())
                .flowCode(flowPlan.getFlowCode())
                .version(version)
                .status(FlowPlanVersionStatuses.DRAFT)
                .graphJson(copyPolicyMap(command.graphJson()))
                .dependencySpecJson(copyPolicyMap(command.dependencySpecJson()))
                .triggerPolicyJson(copyPolicyMap(command.triggerPolicyJson()))
                .confirmationPolicyJson(copyPolicyMap(command.confirmationPolicyJson()))
                .concurrencyPolicyJson(copyPolicyMap(command.concurrencyPolicyJson()))
                .build();

        return flowPlanVersionRepository.save(flowPlanVersion);
    }

    /**
     * Add a schedule node to a draft FlowPlanVersion or return the existing node.
     *
     * @param command node creation command
     * @return newly created or existing ScheduleNode
     */
    public ScheduleNode addNode(CreateScheduleNodeCommand command) {
        FlowPlanVersion flowPlanVersion = getFlowPlanVersionOrThrow(command.flowPlanVersionId());
        requireDraftVersion(flowPlanVersion);

        String nodeCode = requireText(command.nodeCode(), "nodeCode");
        Optional<ScheduleNode> existing = scheduleNodeRepository
                .findByFlowPlanVersionIdAndNodeCode(flowPlanVersion.getId(), nodeCode);
        if (existing.isPresent()) {
            log.debug("ScheduleNode {} already exists in FlowPlanVersion {}", nodeCode, flowPlanVersion.getId());
            return existing.get();
        }

        String nodeType = normalizeNodeType(command.nodeType());
        String outputAssetKey = blankToNull(command.outputAssetKey());
        requireOutputAssetWhenNeeded(nodeType, outputAssetKey);

        ScheduleNode scheduleNode = ScheduleNode.builder()
                .flowPlanVersionId(flowPlanVersion.getId())
                .nodeCode(nodeCode)
                .nodeName(requireText(command.nodeName(), "nodeName"))
                .nodeType(nodeType)
                .dependsOnNodes(copyNodeCodes(command.dependsOnNodes()))
                .inputDependencySpecJson(copyPolicyMap(command.inputDependencySpecJson()))
                .outputAssetKey(outputAssetKey)
                .confirmationPolicyJson(copyPolicyMap(command.confirmationPolicyJson()))
                .sortOrder(command.sortOrder() != null ? command.sortOrder() : 0)
                .build();

        return scheduleNodeRepository.save(scheduleNode);
    }

    /**
     * Publish a FlowPlanVersion and update the owning FlowPlan current version.
     *
     * Publishing a version only affects future scheduling decisions. It does not
     * create workflow/task instances and does not execute downstream work.
     *
     * @param flowPlanVersionId FlowPlanVersion id
     * @param publishedBy user or system identity publishing the version
     * @return published FlowPlanVersion
     */
    public FlowPlanVersion publishVersion(Long flowPlanVersionId, String publishedBy) {
        FlowPlanVersion flowPlanVersion = getFlowPlanVersionOrThrow(flowPlanVersionId);
        List<ScheduleNode> nodes = scheduleNodeRepository
                .findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(flowPlanVersion.getId());
        flowPlanGraphService.validateAndOrder(nodes);
        flowPlanPolicyService.validateVersionPolicies(flowPlanVersion, nodes);

        if (!FlowPlanVersionStatuses.isPublished(flowPlanVersion.getStatus())) {
            flowPlanVersion.markPublished(blankToNull(publishedBy));
            flowPlanVersion = flowPlanVersionRepository.save(flowPlanVersion);
        }

        FlowPlan flowPlan = getFlowPlanOrThrow(flowPlanVersion.getFlowPlanId());
        flowPlan.markPublished(flowPlanVersion.getVersion());
        flowPlanRepository.save(flowPlan);

        return flowPlanVersion;
    }

    /**
     * Find nodes for a FlowPlanVersion in deterministic order.
     *
     * @param flowPlanVersionId FlowPlanVersion id
     * @return ordered schedule nodes
     */
    @Transactional(readOnly = true)
    public List<ScheduleNode> findVersionNodes(Long flowPlanVersionId) {
        return scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(flowPlanVersionId);
    }

    /**
     * Find the latest published version for a FlowPlan.
     *
     * @param flowPlanId FlowPlan id
     * @return newest published version when present
     */
    @Transactional(readOnly = true)
    public Optional<FlowPlanVersion> findLatestPublishedVersion(Long flowPlanId) {
        return flowPlanVersionRepository.findFirstByFlowPlanIdAndStatusOrderByVersionDesc(
                flowPlanId,
                FlowPlanVersionStatuses.PUBLISHED);
    }

    /**
     * Load a FlowPlan or fail with a clear validation error.
     *
     * @param flowPlanId FlowPlan id
     * @return matching FlowPlan
     */
    private FlowPlan getFlowPlanOrThrow(Long flowPlanId) {
        return flowPlanRepository.findById(requireId(flowPlanId, "flowPlanId"))
                .orElseThrow(() -> new IllegalArgumentException("FlowPlan not found: " + flowPlanId));
    }

    /**
     * Load a FlowPlanVersion or fail with a clear validation error.
     *
     * @param flowPlanVersionId FlowPlanVersion id
     * @return matching FlowPlanVersion
     */
    private FlowPlanVersion getFlowPlanVersionOrThrow(Long flowPlanVersionId) {
        return flowPlanVersionRepository.findById(requireId(flowPlanVersionId, "flowPlanVersionId"))
                .orElseThrow(() -> new IllegalArgumentException("FlowPlanVersion not found: " + flowPlanVersionId));
    }

    /**
     * Ensure that nodes are only added to draft versions.
     *
     * @param flowPlanVersion FlowPlanVersion to validate
     */
    private void requireDraftVersion(FlowPlanVersion flowPlanVersion) {
        if (!FlowPlanVersionStatuses.DRAFT.equals(flowPlanVersion.getStatus())) {
            throw new IllegalStateException("Only draft FlowPlanVersion can be changed: " + flowPlanVersion.getId());
        }
    }

    /**
     * Choose the requested version or the next version after the current published one.
     *
     * @param requestedVersion caller-requested version
     * @param flowPlan owning FlowPlan
     * @return normalized positive version number
     */
    private Integer normalizeVersion(Integer requestedVersion, FlowPlan flowPlan) {
        int version = requestedVersion != null
                ? requestedVersion
                : Optional.ofNullable(flowPlan.getCurrentVersion()).orElse(0) + 1;
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        return version;
    }

    /**
     * Validate and normalize a supported node type.
     *
     * @param nodeType raw node type
     * @return normalized node type
     */
    private String normalizeNodeType(String nodeType) {
        String value = requireText(nodeType, "nodeType");
        return switch (value) {
            case ScheduleNodeTypes.ASSET_OUTPUT, ScheduleNodeTypes.CHECKPOINT, ScheduleNodeTypes.SUB_FLOW -> value;
            default -> throw new IllegalArgumentException("Unsupported schedule node type: " + value);
        };
    }

    /**
     * Validate that asset-output nodes declare the target asset used for snapshot confirmation.
     *
     * @param nodeType normalized node type
     * @param outputAssetKey output asset key
     */
    private void requireOutputAssetWhenNeeded(String nodeType, String outputAssetKey) {
        if (ScheduleNodeTypes.requiresOutputAsset(nodeType) && outputAssetKey == null) {
            throw new IllegalArgumentException("ASSET_OUTPUT node requires outputAssetKey");
        }
    }

    /**
     * Normalize a JSON policy map while preserving insertion order for stable tests.
     *
     * @param source source policy map
     * @return copied policy map or an empty map
     */
    private static Map<String, Object> copyPolicyMap(Map<String, Object> source) {
        return source == null || source.isEmpty()
                ? Map.of()
                : new LinkedHashMap<>(source);
    }

    /**
     * Normalize upstream node codes and remove blank values.
     *
     * @param source upstream node codes
     * @return normalized node code list
     */
    private static List<String> copyNodeCodes(List<String> source) {
        if (source == null || source.isEmpty()) {
            return List.of();
        }
        return source.stream()
                .filter(value -> value != null && !value.isBlank())
                .distinct()
                .toList();
    }

    /**
     * Require a positive id value.
     *
     * @param id candidate id
     * @param fieldName field name used in validation messages
     * @return validated id
     */
    private static Long requireId(Long id, String fieldName) {
        if (id == null || id <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return id;
    }

    /**
     * Require a non-blank text value.
     *
     * @param value candidate value
     * @param fieldName field name used in validation messages
     * @return trimmed text
     */
    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /**
     * Convert blank text to null for optional columns.
     *
     * @param value candidate value
     * @return trimmed text or null
     */
    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
