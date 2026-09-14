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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests FlowPlan definition management without invoking execution behavior.
 */
@ExtendWith(MockitoExtension.class)
class FlowPlanServiceTest {

    @Mock
    private FlowPlanRepository flowPlanRepository;

    @Mock
    private FlowPlanVersionRepository flowPlanVersionRepository;

    @Mock
    private ScheduleNodeRepository scheduleNodeRepository;

    @Mock
    private FlowPlanGraphService flowPlanGraphService;

    @Mock
    private FlowPlanPolicyService flowPlanPolicyService;

    @Mock
    private WriterJobBindingService writerJobBindingService;

    @InjectMocks
    private FlowPlanService flowPlanService;

    /**
     * Verify creating a plan records Flow-level ownership and isolation metadata.
     */
    @Test
    void createDraftPlanCreatesDefinitionBoundary() {
        when(flowPlanRepository.findByFlowCode("flow.orders")).thenReturn(Optional.empty());
        when(flowPlanRepository.save(any(FlowPlan.class))).thenAnswer(invocation -> invocation.getArgument(0));

        FlowPlan result = flowPlanService.createDraftPlan(new FlowPlanService.CreateFlowPlanCommand(
                "flow.orders",
                "Orders Flow",
                "finance-space",
                "alice",
                "snapshot driven order aggregation"));

        assertEquals("flow.orders", result.getFlowCode());
        assertEquals("finance-space", result.getFlowSpaceCode());
        assertEquals(FlowPlanStatuses.DRAFT, result.getStatus());
        assertEquals(0, result.getCurrentVersion());
    }

    /**
     * Verify an existing FlowPlan code is idempotent and avoids duplicate definitions.
     */
    @Test
    void createDraftPlanReturnsExistingDefinition() {
        FlowPlan existing = flowPlan(10L, FlowPlanStatuses.PUBLISHED, 3);
        when(flowPlanRepository.findByFlowCode("flow.orders")).thenReturn(Optional.of(existing));

        FlowPlan result = flowPlanService.createDraftPlan(new FlowPlanService.CreateFlowPlanCommand(
                "flow.orders",
                "Orders Flow",
                null,
                "alice",
                null));

        assertEquals(existing, result);
        verify(flowPlanRepository, never()).save(any(FlowPlan.class));
    }

    /**
     * Verify draft versions default to currentVersion + 1 and keep policy JSON.
     */
    @Test
    void createDraftVersionUsesNextVersionAndPolicies() {
        FlowPlan flowPlan = flowPlan(10L, FlowPlanStatuses.PUBLISHED, 3);
        when(flowPlanRepository.findById(10L)).thenReturn(Optional.of(flowPlan));
        when(flowPlanVersionRepository.findByFlowPlanIdAndVersion(10L, 4)).thenReturn(Optional.empty());
        when(flowPlanVersionRepository.save(any(FlowPlanVersion.class))).thenAnswer(invocation -> invocation.getArgument(0));

        FlowPlanVersion result = flowPlanService.createDraftVersion(new FlowPlanService.CreateFlowPlanVersionCommand(
                10L,
                null,
                Map.of("layout", "dag"),
                Map.of("qualityRequired", true),
                Map.of("trigger", "snapshot"),
                Map.of("timeout", "PT1H"),
                Map.of("maxActiveInstances", 1)));

        assertEquals("flow.orders", result.getFlowCode());
        assertEquals(4, result.getVersion());
        assertEquals(FlowPlanVersionStatuses.DRAFT, result.getStatus());
        assertEquals("snapshot", result.getTriggerPolicyJson().get("trigger"));
    }

    /**
     * Verify a node binds dependencies and output asset confirmation metadata.
     */
    @Test
    void addNodeBindsNodeDependenciesAndOutputAsset() {
        FlowPlanVersion version = flowPlanVersion(20L, FlowPlanVersionStatuses.DRAFT);
        when(flowPlanVersionRepository.findById(20L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdAndNodeCode(20L, "node.dwd_orders"))
                .thenReturn(Optional.empty());
        when(scheduleNodeRepository.save(any(ScheduleNode.class))).thenAnswer(invocation -> invocation.getArgument(0));

        ScheduleNode result = flowPlanService.addNode(new FlowPlanService.CreateScheduleNodeCommand(
                20L,
                "node.dwd_orders",
                "DWD Orders",
                ScheduleNodeTypes.ASSET_OUTPUT,
                "STREAMING",
                List.of("node.ods_orders", "node.ods_orders", " "),
                Map.of("assetKey", "paimon.ods.orders"),
                "paimon.dwd.orders",
                "writer.dwd.orders",
                Map.of("mode", "snapshot_advance"),
                10));

        assertEquals(List.of("node.ods_orders"), result.getDependsOnNodes());
        assertEquals("STREAMING", result.getProcessingMode());
        assertEquals("paimon.dwd.orders", result.getOutputAssetKey());
        assertEquals("writer.dwd.orders", result.getWriterJobKey());
        assertEquals("snapshot_advance", result.getConfirmationPolicyJson().get("mode"));
        assertEquals(10, result.getSortOrder());
    }

    /**
     * Verify asset-output nodes must declare the target asset used by snapshot confirmation.
     */
    @Test
    void addNodeRejectsAssetOutputWithoutOutputAsset() {
        FlowPlanVersion version = flowPlanVersion(20L, FlowPlanVersionStatuses.DRAFT);
        when(flowPlanVersionRepository.findById(20L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdAndNodeCode(20L, "node.bad"))
                .thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () -> flowPlanService.addNode(
                new FlowPlanService.CreateScheduleNodeCommand(
                        20L,
                        "node.bad",
                        "Bad Node",
                        ScheduleNodeTypes.ASSET_OUTPUT,
                        "BATCH",
                        List.of(),
                        Map.of(),
                        " ",
                        "writer.bad",
                        Map.of(),
                        1)));
    }

    /**
     * Verify publishing a version updates both version status and FlowPlan current version.
     */
    @Test
    void publishVersionUpdatesDefinitionAnchorWithoutExecutingWork() {
        FlowPlanVersion version = flowPlanVersion(20L, FlowPlanVersionStatuses.DRAFT);
        FlowPlan flowPlan = flowPlan(10L, FlowPlanStatuses.DRAFT, 0);
        when(flowPlanVersionRepository.findById(20L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(20L))
                .thenReturn(List.of(scheduleNode(30L)));
        when(flowPlanGraphService.validateAndOrder(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanVersionRepository.save(any(FlowPlanVersion.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(flowPlanRepository.findById(10L)).thenReturn(Optional.of(flowPlan));
        when(flowPlanRepository.save(any(FlowPlan.class))).thenAnswer(invocation -> invocation.getArgument(0));

        FlowPlanVersion result = flowPlanService.publishVersion(20L, "alice");

        assertEquals(FlowPlanVersionStatuses.PUBLISHED, result.getStatus());
        assertEquals("alice", result.getPublishedBy());
        assertEquals(FlowPlanStatuses.PUBLISHED, flowPlan.getStatus());
        assertEquals(4, flowPlan.getCurrentVersion());
        verify(flowPlanVersionRepository).save(version);
        verify(flowPlanRepository).save(flowPlan);
        verify(flowPlanGraphService).validateAndOrder(any());
        verify(flowPlanPolicyService).validateVersionPolicies(any(), any());
        verify(writerJobBindingService).validatePublishedNodes(any());
    }

    /** Verify an invalid runtime policy cannot become a published definition. */
    @Test
    void publishVersionRejectsInvalidRuntimePolicyBeforeStateChange() {
        FlowPlanVersion version = flowPlanVersion(20L, FlowPlanVersionStatuses.DRAFT);
        List<ScheduleNode> nodes = List.of(scheduleNode(30L));
        when(flowPlanVersionRepository.findById(20L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(20L))
                .thenReturn(nodes);
        when(flowPlanGraphService.validateAndOrder(nodes)).thenReturn(nodes);
        doThrow(new IllegalArgumentException("invalid policy"))
                .when(flowPlanPolicyService).validateVersionPolicies(version, nodes);

        assertThrows(
                IllegalArgumentException.class,
                () -> flowPlanService.publishVersion(20L, "alice"));

        assertEquals(FlowPlanVersionStatuses.DRAFT, version.getStatus());
        verify(flowPlanVersionRepository, never()).save(any());
        verify(flowPlanRepository, never()).save(any());
    }

    /**
     * Verify version-node lookup keeps repository ordering.
     */
    @Test
    void findVersionNodesReturnsOrderedRepositoryResult() {
        List<ScheduleNode> nodes = List.of(scheduleNode(30L));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(20L))
                .thenReturn(nodes);

        List<ScheduleNode> result = flowPlanService.findVersionNodes(20L);

        assertSame(nodes, result);
    }

    /**
     * Verify latest-published lookup uses published-version status.
     */
    @Test
    void findLatestPublishedVersionReturnsRepositoryResult() {
        FlowPlanVersion published = flowPlanVersion(20L, FlowPlanVersionStatuses.PUBLISHED);
        when(flowPlanVersionRepository.findFirstByFlowPlanIdAndStatusOrderByVersionDesc(
                10L,
                FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(Optional.of(published));

        Optional<FlowPlanVersion> result = flowPlanService.findLatestPublishedVersion(10L);

        assertTrue(result.isPresent());
        assertSame(published, result.get());
    }

    /**
     * Build a FlowPlan fixture.
     */
    private FlowPlan flowPlan(Long id, String status, Integer currentVersion) {
        return FlowPlan.builder()
                .id(id)
                .flowCode("flow.orders")
                .flowName("Orders Flow")
                .status(status)
                .currentVersion(currentVersion)
                .build();
    }

    /**
     * Build a FlowPlanVersion fixture.
     */
    private FlowPlanVersion flowPlanVersion(Long id, String status) {
        return FlowPlanVersion.builder()
                .id(id)
                .flowPlanId(10L)
                .flowCode("flow.orders")
                .version(4)
                .status(status)
                .build();
    }

    /**
     * Build a ScheduleNode fixture.
     */
    private ScheduleNode scheduleNode(Long id) {
        return ScheduleNode.builder()
                .id(id)
                .flowPlanVersionId(20L)
                .nodeCode("node.dwd_orders")
                .nodeName("DWD Orders")
                .nodeType(ScheduleNodeTypes.ASSET_OUTPUT)
                .outputAssetKey("paimon.dwd.orders")
                .writerJobKey("writer.dwd.orders")
                .build();
    }
}
