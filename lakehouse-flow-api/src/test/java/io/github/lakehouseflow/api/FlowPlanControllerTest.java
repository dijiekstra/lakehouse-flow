package io.github.lakehouseflow.api;

import io.github.lakehouseflow.api.dto.CreateFlowPlanRequest;
import io.github.lakehouseflow.api.dto.CreateFlowPlanVersionRequest;
import io.github.lakehouseflow.api.dto.CreateScheduleNodeRequest;
import io.github.lakehouseflow.api.dto.FlowPlanResponse;
import io.github.lakehouseflow.api.dto.FlowPlanVersionResponse;
import io.github.lakehouseflow.api.dto.PublishFlowPlanVersionRequest;
import io.github.lakehouseflow.api.dto.ScheduleNodeResponse;
import io.github.lakehouseflow.common.FlowPlanStatuses;
import io.github.lakehouseflow.common.FlowPlanVersionStatuses;
import io.github.lakehouseflow.common.ScheduleNodeTypes;
import io.github.lakehouseflow.model.FlowPlan;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.service.FlowPlanService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests FlowPlanController mapping for FlowPlan, version, and node APIs.
 */
@ExtendWith(MockitoExtension.class)
class FlowPlanControllerTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 9, 12, 10, 0);

    @Mock
    private FlowPlanService flowPlanService;

    /**
     * Verify FlowPlan draft creation delegates to the service and returns response fields.
     */
    @Test
    void createDraftPlanMapsRequestAndResponse() {
        FlowPlanController controller = new FlowPlanController(flowPlanService);
        FlowPlan flowPlan = flowPlan();
        when(flowPlanService.createDraftPlan(org.mockito.ArgumentMatchers.any()))
                .thenReturn(flowPlan);

        FlowPlanResponse response = controller.createDraftPlan(new CreateFlowPlanRequest(
                "orders_flow",
                "Orders Flow",
                "analytics",
                "alice",
                "daily orders"));

        ArgumentCaptor<FlowPlanService.CreateFlowPlanCommand> captor =
                ArgumentCaptor.forClass(FlowPlanService.CreateFlowPlanCommand.class);
        verify(flowPlanService).createDraftPlan(captor.capture());
        assertEquals("orders_flow", captor.getValue().flowCode());
        assertEquals("Orders Flow", response.flowName());
        assertEquals(1, response.currentVersion());
    }

    /**
     * Verify FlowPlan version creation maps policy JSON fields into the service command.
     */
    @Test
    void createDraftVersionMapsPolicyFields() {
        FlowPlanController controller = new FlowPlanController(flowPlanService);
        FlowPlanVersion version = flowPlanVersion();
        when(flowPlanService.createDraftVersion(org.mockito.ArgumentMatchers.any()))
                .thenReturn(version);

        FlowPlanVersionResponse response = controller.createDraftVersion(
                7L,
                new CreateFlowPlanVersionRequest(
                        2,
                        Map.of("layout", "dag"),
                        Map.of("deps", "snapshot"),
                        Map.of("cron", "0 0 * * *"),
                        Map.of("timeoutMinutes", 30),
                        Map.of("maxParallelism", 4)));

        ArgumentCaptor<FlowPlanService.CreateFlowPlanVersionCommand> captor =
                ArgumentCaptor.forClass(FlowPlanService.CreateFlowPlanVersionCommand.class);
        verify(flowPlanService).createDraftVersion(captor.capture());
        assertEquals(7L, captor.getValue().flowPlanId());
        assertEquals(2, captor.getValue().version());
        assertEquals("orders_flow", response.flowCode());
    }

    /**
     * Verify node creation exposes ScheduleNode metadata without executor details.
     */
    @Test
    void addNodeMapsScheduleNodeRequest() {
        FlowPlanController controller = new FlowPlanController(flowPlanService);
        ScheduleNode node = scheduleNode();
        when(flowPlanService.addNode(org.mockito.ArgumentMatchers.any()))
                .thenReturn(node);

        ScheduleNodeResponse response = controller.addNode(
                9L,
                new CreateScheduleNodeRequest(
                        "dwd_orders",
                        "DWD Orders",
                        ScheduleNodeTypes.ASSET_OUTPUT,
                        "STREAMING",
                        List.of("ods_orders"),
                        Map.of("asset", "paimon.ods.orders"),
                        "paimon.dwd.orders",
                        "writer.dwd.orders",
                        Map.of("timeoutMinutes", 30),
                        20));

        ArgumentCaptor<FlowPlanService.CreateScheduleNodeCommand> captor =
                ArgumentCaptor.forClass(FlowPlanService.CreateScheduleNodeCommand.class);
        verify(flowPlanService).addNode(captor.capture());
        assertEquals(9L, captor.getValue().flowPlanVersionId());
        assertEquals("STREAMING", captor.getValue().processingMode());
        assertEquals("dwd_orders", response.nodeCode());
        assertEquals("paimon.dwd.orders", response.outputAssetKey());
        assertEquals("writer.dwd.orders", response.writerJobKey());
    }

    /**
     * Verify publication maps the publisher and returns the published version.
     */
    @Test
    void publishVersionMapsPublisher() {
        FlowPlanController controller = new FlowPlanController(flowPlanService);
        FlowPlanVersion version = flowPlanVersion();
        when(flowPlanService.publishVersion(9L, "alice")).thenReturn(version);

        FlowPlanVersionResponse response = controller.publishVersion(
                9L,
                new PublishFlowPlanVersionRequest("alice"));

        assertEquals(9L, response.id());
        assertEquals(FlowPlanVersionStatuses.PUBLISHED, response.status());
    }

    /**
     * Verify node listing returns ordered node responses from the service.
     */
    @Test
    void listVersionNodesReturnsNodes() {
        FlowPlanController controller = new FlowPlanController(flowPlanService);
        when(flowPlanService.findVersionNodes(9L)).thenReturn(List.of(scheduleNode()));

        List<ScheduleNodeResponse> responses = controller.listVersionNodes(9L);

        assertEquals(1, responses.size());
        assertEquals("dwd_orders", responses.get(0).nodeCode());
    }

    /**
     * Verify latest published version returns 200 when a published version exists.
     */
    @Test
    void getLatestPublishedVersionReturnsPublishedVersion() {
        FlowPlanController controller = new FlowPlanController(flowPlanService);
        when(flowPlanService.findLatestPublishedVersion(7L)).thenReturn(Optional.of(flowPlanVersion()));

        ResponseEntity<FlowPlanVersionResponse> response = controller.getLatestPublishedVersion(7L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(2, response.getBody().version());
    }

    /**
     * Verify latest published version returns 404 when no version has been published.
     */
    @Test
    void getLatestPublishedVersionReturnsNotFoundWhenAbsent() {
        FlowPlanController controller = new FlowPlanController(flowPlanService);
        when(flowPlanService.findLatestPublishedVersion(7L)).thenReturn(Optional.empty());

        ResponseEntity<FlowPlanVersionResponse> response = controller.getLatestPublishedVersion(7L);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    /**
     * Build a FlowPlan fixture.
     */
    private FlowPlan flowPlan() {
        return FlowPlan.builder()
                .id(7L)
                .flowCode("orders_flow")
                .flowName("Orders Flow")
                .flowSpaceCode("analytics")
                .owner("alice")
                .description("daily orders")
                .status(FlowPlanStatuses.PUBLISHED)
                .currentVersion(1)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }

    /**
     * Build a FlowPlanVersion fixture.
     */
    private FlowPlanVersion flowPlanVersion() {
        return FlowPlanVersion.builder()
                .id(9L)
                .flowPlanId(7L)
                .flowCode("orders_flow")
                .version(2)
                .status(FlowPlanVersionStatuses.PUBLISHED)
                .graphJson(Map.of("layout", "dag"))
                .dependencySpecJson(Map.of("deps", "snapshot"))
                .triggerPolicyJson(Map.of("cron", "0 0 * * *"))
                .confirmationPolicyJson(Map.of("timeoutMinutes", 30))
                .concurrencyPolicyJson(Map.of("maxParallelism", 4))
                .publishedBy("alice")
                .publishedAt(NOW)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }

    /**
     * Build a ScheduleNode fixture.
     */
    private ScheduleNode scheduleNode() {
        return ScheduleNode.builder()
                .id(11L)
                .flowPlanVersionId(9L)
                .nodeCode("dwd_orders")
                .nodeName("DWD Orders")
                .nodeType(ScheduleNodeTypes.ASSET_OUTPUT)
                .dependsOnNodes(List.of("ods_orders"))
                .inputDependencySpecJson(Map.of("asset", "paimon.ods.orders"))
                .outputAssetKey("paimon.dwd.orders")
                .writerJobKey("writer.dwd.orders")
                .confirmationPolicyJson(Map.of("timeoutMinutes", 30))
                .sortOrder(20)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }
}
