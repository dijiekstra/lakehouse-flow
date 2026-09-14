package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillCascadePolicies;
import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.model.ScheduleNode;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates and traverses immutable FlowPlan scheduling graphs.
 */
@Service
public class FlowPlanGraphService {

    /**
     * Validate node references and cycles, then return a stable topological order.
     *
     * @param nodes nodes in definition presentation order
     * @return nodes in stable dependency order
     */
    public List<ScheduleNode> validateAndOrder(List<ScheduleNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("FlowPlanVersion must contain at least one schedule node");
        }

        Map<String, ScheduleNode> byCode = indexNodes(nodes);
        nodes.forEach(node -> ScheduleNodeProcessingModes.normalize(node.getProcessingMode()));
        Map<String, Integer> indegrees = new LinkedHashMap<>();
        Map<String, List<String>> downstreamCodes = new HashMap<>();
        byCode.keySet().forEach(code -> indegrees.put(code, 0));

        for (ScheduleNode node : nodes) {
            for (String dependencyCode : dependencies(node)) {
                if (!byCode.containsKey(dependencyCode)) {
                    throw new IllegalArgumentException(
                            "ScheduleNode " + node.getNodeCode() + " depends on missing node " + dependencyCode);
                }
                if (node.getNodeCode().equals(dependencyCode)) {
                    throw new IllegalArgumentException("ScheduleNode cannot depend on itself: " + dependencyCode);
                }
                indegrees.compute(node.getNodeCode(), (ignored, count) -> count + 1);
                downstreamCodes.computeIfAbsent(dependencyCode, ignored -> new ArrayList<>())
                        .add(node.getNodeCode());
            }
        }

        List<ScheduleNode> ordered = new ArrayList<>();
        Set<String> emitted = new HashSet<>();
        while (ordered.size() < nodes.size()) {
            ScheduleNode next = nodes.stream()
                    .filter(node -> !emitted.contains(node.getNodeCode()))
                    .filter(node -> indegrees.get(node.getNodeCode()) == 0)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("FlowPlanVersion graph contains a dependency cycle"));
            ordered.add(next);
            emitted.add(next.getNodeCode());
            downstreamCodes.getOrDefault(next.getNodeCode(), List.of())
                    .forEach(code -> indegrees.compute(code, (ignored, count) -> count - 1));
        }
        return List.copyOf(ordered);
    }

    /**
     * Resolve a node and optional downstream scope in stable topological order.
     *
     * @param nodes complete FlowPlanVersion graph
     * @param startNodeCode selected operation entry node
     * @param cascadePolicy normalized or caller-supplied cascade policy
     * @return selected graph scope
     */
    public List<ScheduleNode> selectSubgraph(
            List<ScheduleNode> nodes,
            String startNodeCode,
            String cascadePolicy) {

        List<ScheduleNode> ordered = validateAndOrder(nodes);
        ScheduleNode startNode = findNode(ordered, startNodeCode);
        String normalizedPolicy = BackfillCascadePolicies.normalize(cascadePolicy);
        if (BackfillCascadePolicies.NO_CASCADE.equals(normalizedPolicy)) {
            return List.of(startNode);
        }
        if (BackfillCascadePolicies.DIRECT_DOWNSTREAM.equals(normalizedPolicy)) {
            List<ScheduleNode> selected = ordered.stream()
                    .filter(node -> node.getNodeCode().equals(startNodeCode)
                            || dependencies(node).contains(startNodeCode))
                    .toList();
            validateDependencyClosedScope(selected, startNodeCode);
            return selected;
        }

        Set<String> selected = new LinkedHashSet<>();
        selected.add(startNodeCode);
        boolean changed;
        do {
            changed = false;
            for (ScheduleNode node : ordered) {
                boolean directMatch = dependencies(node).stream().anyMatch(selected::contains);
                if (directMatch && selected.add(node.getNodeCode())) {
                    changed = true;
                }
            }
        } while (changed);

        List<ScheduleNode> selectedNodes = ordered.stream()
                .filter(node -> selected.contains(node.getNodeCode()))
                .toList();
        validateDependencyClosedScope(selectedNodes, startNodeCode);
        return selectedNodes;
    }

    /**
     * Find a node by code in a validated graph.
     *
     * @param nodes graph nodes
     * @param nodeCode target node code
     * @return matching node
     */
    public ScheduleNode findNode(List<ScheduleNode> nodes, String nodeCode) {
        if (nodeCode == null || nodeCode.isBlank()) {
            throw new IllegalArgumentException("nodeCode is required");
        }
        return nodes.stream()
                .filter(node -> nodeCode.trim().equals(node.getNodeCode()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("ScheduleNode not found: " + nodeCode));
    }

    /**
     * Index graph nodes while rejecting duplicate or blank identities.
     *
     * @param nodes source nodes
     * @return insertion-ordered node map
     */
    private Map<String, ScheduleNode> indexNodes(List<ScheduleNode> nodes) {
        Map<String, ScheduleNode> byCode = new LinkedHashMap<>();
        for (ScheduleNode node : nodes) {
            if (node == null || node.getNodeCode() == null || node.getNodeCode().isBlank()) {
                throw new IllegalArgumentException("ScheduleNode code is required");
            }
            if (byCode.putIfAbsent(node.getNodeCode(), node) != null) {
                throw new IllegalArgumentException("Duplicate ScheduleNode code: " + node.getNodeCode());
            }
        }
        return byCode;
    }

    /**
     * Require every selected downstream node to retain all of its direct DAG parents.
     *
     * The selected start node is the only deliberate dependency bypass. Rejecting an
     * incomplete join here avoids creating a waiting intent that can never obtain
     * same-instance snapshot confirmation for an omitted parent.
     *
     * @param selectedNodes operation scope selected from the complete graph
     * @param startNodeCode explicit operation entry node
     */
    private void validateDependencyClosedScope(List<ScheduleNode> selectedNodes, String startNodeCode) {
        Set<String> selectedCodes = selectedNodes.stream()
                .map(ScheduleNode::getNodeCode)
                .collect(Collectors.toSet());
        for (ScheduleNode node : selectedNodes) {
            if (startNodeCode.equals(node.getNodeCode())) {
                continue;
            }
            List<String> missingDependencies = dependencies(node).stream()
                    .filter(code -> !selectedCodes.contains(code))
                    .toList();
            if (!missingDependencies.isEmpty()) {
                throw new IllegalArgumentException(
                        "Selected backfill scope for node " + node.getNodeCode()
                                + " omits direct upstream nodes " + String.join(",", missingDependencies));
            }
        }
    }

    /**
     * Return normalized upstream codes for a node.
     *
     * @param node graph node
     * @return distinct dependency codes
     */
    private List<String> dependencies(ScheduleNode node) {
        if (node.getDependsOnNodes() == null) {
            return List.of();
        }
        return node.getDependsOnNodes().stream()
                .filter(code -> code != null && !code.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
    }
}
