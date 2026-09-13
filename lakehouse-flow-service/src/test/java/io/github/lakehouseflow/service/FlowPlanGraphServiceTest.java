package io.github.lakehouseflow.service;

import io.github.lakehouseflow.model.ScheduleNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests FlowPlan graph validation and deterministic downstream traversal.
 */
class FlowPlanGraphServiceTest {

    private final FlowPlanGraphService flowPlanGraphService = new FlowPlanGraphService();

    /**
     * Verify graph validation returns a stable topological order.
     */
    @Test
    void validateAndOrderSortsDependenciesBeforeDownstreamNodes() {
        ScheduleNode root = node("root", List.of());
        ScheduleNode middle = node("middle", List.of("root"));
        ScheduleNode leaf = node("leaf", List.of("middle"));

        List<ScheduleNode> ordered = flowPlanGraphService.validateAndOrder(List.of(leaf, root, middle));

        assertEquals(List.of("root", "middle", "leaf"), ordered.stream().map(ScheduleNode::getNodeCode).toList());
    }

    /**
     * Verify invalid references, self references, duplicates, and cycles are rejected.
     */
    @Test
    void validateAndOrderRejectsInvalidGraphs() {
        assertThrows(IllegalArgumentException.class, () -> flowPlanGraphService.validateAndOrder(List.of()));
        assertThrows(IllegalArgumentException.class, () -> flowPlanGraphService.validateAndOrder(
                List.of(node("child", List.of("missing")))));
        assertThrows(IllegalArgumentException.class, () -> flowPlanGraphService.validateAndOrder(
                List.of(node("self", List.of("self")))));
        assertThrows(IllegalArgumentException.class, () -> flowPlanGraphService.validateAndOrder(
                List.of(node("same", List.of()), node("same", List.of()))));
        assertThrows(IllegalArgumentException.class, () -> flowPlanGraphService.validateAndOrder(
                List.of(node("left", List.of("right")), node("right", List.of("left")))));
    }

    /**
     * Verify direct and transitive cascade scopes preserve topological order.
     */
    @Test
    void selectSubgraphAppliesCascadePolicy() {
        List<ScheduleNode> nodes = List.of(
                node("root", List.of()),
                node("middle", List.of("root")),
                node("leaf", List.of("middle")),
                node("side", List.of()));

        assertEquals(List.of("middle"), codes(flowPlanGraphService.selectSubgraph(nodes, "middle", "NO_CASCADE")));
        assertEquals(
                List.of("root", "middle"),
                codes(flowPlanGraphService.selectSubgraph(nodes, "root", "DIRECT_DOWNSTREAM")));
        assertEquals(
                List.of("root", "middle", "leaf"),
                codes(flowPlanGraphService.selectSubgraph(nodes, "root", "TRANSITIVE_DOWNSTREAM")));
    }

    /**
     * Verify a downstream join cannot omit another direct parent from the backfill scope.
     */
    @Test
    void selectSubgraphRejectsDependencyIncompleteJoin() {
        ScheduleNode selectedRoot = node("selected-root", List.of());
        ScheduleNode omittedRoot = node("omitted-root", List.of());
        ScheduleNode join = node("join", List.of("selected-root", "omitted-root"));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> flowPlanGraphService.selectSubgraph(
                        List.of(selectedRoot, omittedRoot, join),
                        "selected-root",
                        "TRANSITIVE_DOWNSTREAM"));

        assertTrue(error.getMessage().contains("omitted-root"));
    }

    /**
     * Verify node lookup validates blank and missing node identities.
     */
    @Test
    void findNodeReturnsMatchAndRejectsUnknownCode() {
        ScheduleNode root = node("root", List.of());

        assertEquals(root, flowPlanGraphService.findNode(List.of(root), "root"));
        assertThrows(IllegalArgumentException.class, () -> flowPlanGraphService.findNode(List.of(root), " "));
        assertThrows(IllegalArgumentException.class, () -> flowPlanGraphService.findNode(List.of(root), "missing"));
    }

    /**
     * Convert nodes into their stable codes for concise assertions.
     *
     * @param nodes schedule nodes
     * @return node codes
     */
    private List<String> codes(List<ScheduleNode> nodes) {
        return nodes.stream().map(ScheduleNode::getNodeCode).toList();
    }

    /**
     * Build a graph node fixture.
     *
     * @param code node code
     * @param dependencies upstream node codes
     * @return schedule node
     */
    private ScheduleNode node(String code, List<String> dependencies) {
        return ScheduleNode.builder().nodeCode(code).dependsOnNodes(dependencies).build();
    }
}
