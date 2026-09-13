package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.FlowPlanVersionStatuses;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.TriggerHistory;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests natural snapshot triggering against published FlowPlan DAG definitions.
 */
@ExtendWith(MockitoExtension.class)
class FlowPlanEvaluationServiceTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 10, 0);
    private static final String ASSET = "orders.dt=2026-09-12";

    @Mock
    private FlowPlanVersionRepository flowPlanVersionRepository;

    @Mock
    private ScheduleNodeRepository scheduleNodeRepository;

    @Mock
    private FlowPlanGraphService flowPlanGraphService;

    @Mock
    private FlowPlanConditionService flowPlanConditionService;

    @Mock
    private SchedulingTemplateResolver schedulingTemplateResolver;

    @Mock
    private WorkflowInstanceService workflowInstanceService;

    @Mock
    private TaskInstanceService taskInstanceService;

    @Mock
    private TriggerHistoryService triggerHistoryService;

    @Mock
    private FlowPlanDecisionMetrics flowPlanDecisionMetrics;

    @InjectMocks
    private FlowPlanEvaluationService flowPlanEvaluationService;

    /**
     * Verify a matching snapshot emits roots as ready and downstream nodes as waiting.
     */
    @Test
    void evaluateTriggeredAssetEmitsPublishedDagSchedulingIntents() {
        FlowPlanVersion version = version(Map.of());
        ScheduleNode root = node(61L, "root", List.of(), rootDependency(), "dwd.dt=${bizDate}");
        ScheduleNode leaf = node(62L, "leaf", List.of("root"), Map.of(), "ads.dt=${bizDate}");
        WorkflowInstance workflow = WorkflowInstance.builder().id(11L).build();
        TaskInstance rootTask = TaskInstance.builder().id(71L).build();
        TaskInstance leafTask = TaskInstance.builder().id(72L).build();
        when(flowPlanVersionRepository.findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(List.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(root, leaf));
        when(flowPlanGraphService.validateAndOrder(List.of(root, leaf))).thenReturn(List.of(root, leaf));
        when(flowPlanConditionService.referencesAsset(rootDependency(), BIZ_DATE.toLocalDate(), ASSET))
                .thenReturn(true);
        when(triggerHistoryService.findByTriggerKey(anyString())).thenReturn(Optional.empty());
        when(flowPlanConditionService.evaluate(rootDependency(), BIZ_DATE.toLocalDate()))
                .thenReturn(EvaluationResult.satisfied());
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(4),
                eq(BIZ_DATE),
                eq("SNAPSHOT_DRIVEN"),
                anyString(),
                anyString(),
                eq(51L))).thenReturn(workflow);
        when(schedulingTemplateResolver.resolve("dwd.dt=${bizDate}", BIZ_DATE.toLocalDate()))
                .thenReturn("dwd.dt=2026-09-12");
        when(schedulingTemplateResolver.resolve("ads.dt=${bizDate}", BIZ_DATE.toLocalDate()))
                .thenReturn("ads.dt=2026-09-12");
        when(taskInstanceService.createInstance(
                11L, "root", 4, BIZ_DATE.toLocalDate().atStartOfDay(),
                "dwd.dt=2026-09-12", 51L, 61L)).thenReturn(rootTask);
        when(taskInstanceService.createInstance(
                11L, "leaf", 4, BIZ_DATE.toLocalDate().atStartOfDay(),
                "ads.dt=2026-09-12", 51L, 62L)).thenReturn(leafTask);

        List<FlowPlanEvaluationService.FlowPlanTriggerOutcome> results =
                flowPlanEvaluationService.evaluateTriggeredAsset(ASSET, "101", BIZ_DATE);

        assertEquals(1, results.size());
        assertTrue(results.get(0).schedulingIntentEmitted());
        assertEquals(11L, results.get(0).workflowInstanceId());
        verify(taskInstanceService).markSchedulable(71L);
        verify(taskInstanceService).markWaitingForSnapshot(
                72L,
                "Waiting for upstream snapshot confirmation: root");
        verify(flowPlanDecisionMetrics).recordInspection(FlowPlanInspectionOutcome.MATCHED);
        verify(flowPlanDecisionMetrics).recordDecision(eq(FlowPlanTriggerDecision.EMITTED), any(Duration.class));
    }

    /**
     * Verify table and partition projections evaluate one FlowPlan version only once.
     */
    @Test
    void evaluateTriggeredAssetsUsesMatchingPartitionWithoutDuplicateEmission() {
        FlowPlanVersion version = version(Map.of());
        ScheduleNode root = node(61L, "root", List.of(), rootDependency(), "dwd.dt=${bizDate}");
        WorkflowInstance workflow = WorkflowInstance.builder().id(11L).build();
        TaskInstance rootTask = TaskInstance.builder().id(71L).build();
        when(flowPlanVersionRepository.findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(List.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(root));
        when(flowPlanGraphService.validateAndOrder(List.of(root))).thenReturn(List.of(root));
        when(flowPlanConditionService.referencesAsset(rootDependency(), BIZ_DATE.toLocalDate(), "orders"))
                .thenReturn(false);
        when(flowPlanConditionService.referencesAsset(rootDependency(), BIZ_DATE.toLocalDate(), ASSET))
                .thenReturn(true);
        when(triggerHistoryService.findByTriggerKey(anyString())).thenReturn(Optional.empty());
        when(flowPlanConditionService.evaluate(rootDependency(), BIZ_DATE.toLocalDate()))
                .thenReturn(EvaluationResult.satisfied());
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(4),
                eq(BIZ_DATE),
                eq("SNAPSHOT_DRIVEN"),
                anyString(),
                anyString(),
                eq(51L))).thenReturn(workflow);
        when(schedulingTemplateResolver.resolve("dwd.dt=${bizDate}", BIZ_DATE.toLocalDate()))
                .thenReturn("dwd.dt=2026-09-12");
        when(taskInstanceService.createInstance(
                11L, "root", 4, BIZ_DATE.toLocalDate().atStartOfDay(),
                "dwd.dt=2026-09-12", 51L, 61L)).thenReturn(rootTask);

        List<FlowPlanEvaluationService.FlowPlanTriggerOutcome> results =
                flowPlanEvaluationService.evaluateTriggeredAssets(
                        List.of("orders", ASSET, ASSET),
                        "101",
                        BIZ_DATE);

        assertEquals(1, results.size());
        assertTrue(results.get(0).schedulingIntentEmitted());
        verify(workflowInstanceService, times(1)).createInstance(
                any(), any(), any(), any(), any(), any(), any());
    }

    /**
     * Verify one unsatisfied root dependency records a skipped decision.
     */
    @Test
    void evaluateTriggeredAssetSkipsWhenRootDependencyIsUnsatisfied() {
        FlowPlanVersion version = version(Map.of());
        ScheduleNode root = node(61L, "root", List.of(), rootDependency(), "dwd");
        when(flowPlanVersionRepository.findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(List.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(root));
        when(flowPlanGraphService.validateAndOrder(List.of(root))).thenReturn(List.of(root));
        when(flowPlanConditionService.referencesAsset(rootDependency(), BIZ_DATE.toLocalDate(), ASSET))
                .thenReturn(true);
        when(triggerHistoryService.findByTriggerKey(anyString())).thenReturn(Optional.empty());
        when(flowPlanConditionService.evaluate(rootDependency(), BIZ_DATE.toLocalDate()))
                .thenReturn(EvaluationResult.unsatisfied("waiting payments"));

        List<FlowPlanEvaluationService.FlowPlanTriggerOutcome> results =
                flowPlanEvaluationService.evaluateTriggeredAsset(ASSET, "101", BIZ_DATE);

        assertFalse(results.get(0).schedulingIntentEmitted());
        assertEquals("waiting payments", results.get(0).reason());
        verify(workflowInstanceService, never()).createInstance(any(), any(), any(), any(), any(), any(), any());
        verify(flowPlanDecisionMetrics).recordDecision(
                eq(FlowPlanTriggerDecision.BLOCKED_CONDITION),
                any(Duration.class));
    }

    /**
     * Verify an existing trigger key deduplicates a repeated snapshot event.
     */
    @Test
    void evaluateTriggeredAssetDeduplicatesExistingDecision() {
        FlowPlanVersion version = version(Map.of());
        ScheduleNode root = node(61L, "root", List.of(), rootDependency(), "dwd");
        when(flowPlanVersionRepository.findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(List.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(root));
        when(flowPlanGraphService.validateAndOrder(List.of(root))).thenReturn(List.of(root));
        when(flowPlanConditionService.referencesAsset(rootDependency(), BIZ_DATE.toLocalDate(), ASSET))
                .thenReturn(true);
        when(triggerHistoryService.findByTriggerKey(anyString()))
                .thenReturn(Optional.of(TriggerHistory.builder().workflowInstanceId(11L).build()));

        List<FlowPlanEvaluationService.FlowPlanTriggerOutcome> results =
                flowPlanEvaluationService.evaluateTriggeredAsset(ASSET, "101", BIZ_DATE);

        assertFalse(results.get(0).schedulingIntentEmitted());
        assertEquals(11L, results.get(0).workflowInstanceId());
        verify(flowPlanConditionService, never()).evaluate(any(), any());
        verify(flowPlanDecisionMetrics).recordDecision(
                eq(FlowPlanTriggerDecision.DEDUPLICATED),
                any(Duration.class));
    }

    /**
     * Verify a plan explicitly marked MANUAL ignores natural snapshot events.
     */
    @Test
    void evaluateTriggeredAssetIgnoresManualTriggerPolicy() {
        FlowPlanVersion version = version(Map.of("type", "MANUAL"));
        when(flowPlanVersionRepository.findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(List.of(version));

        List<FlowPlanEvaluationService.FlowPlanTriggerOutcome> results =
                flowPlanEvaluationService.evaluateTriggeredAsset(ASSET, "101", BIZ_DATE);

        assertTrue(results.isEmpty());
        verify(scheduleNodeRepository, never()).findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L);
        verify(flowPlanDecisionMetrics).recordInspection(FlowPlanInspectionOutcome.TRIGGER_POLICY_FILTERED);
    }

    /** Verify an unrelated changed asset is observable without creating a trigger decision. */
    @Test
    void evaluateTriggeredAssetRecordsUnmatchedInspection() {
        FlowPlanVersion version = version(Map.of());
        ScheduleNode root = node(61L, "root", List.of(), rootDependency(), "dwd");
        when(flowPlanVersionRepository.findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(List.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(root));
        when(flowPlanGraphService.validateAndOrder(List.of(root))).thenReturn(List.of(root));
        when(flowPlanConditionService.referencesAsset(rootDependency(), BIZ_DATE.toLocalDate(), ASSET))
                .thenReturn(false);

        List<FlowPlanEvaluationService.FlowPlanTriggerOutcome> results =
                flowPlanEvaluationService.evaluateTriggeredAsset(ASSET, "101", BIZ_DATE);

        assertTrue(results.isEmpty());
        verify(flowPlanDecisionMetrics).recordInspection(FlowPlanInspectionOutcome.ASSET_UNMATCHED);
        verify(flowPlanDecisionMetrics, never()).recordDecision(any(), any());
    }

    /** Verify evaluation failures remain visible and propagate to the ingestion transaction. */
    @Test
    void evaluateTriggeredAssetRecordsAndRethrowsFailure() {
        FlowPlanVersion version = version(Map.of());
        ScheduleNode root = node(61L, "root", List.of(), rootDependency(), "dwd");
        when(flowPlanVersionRepository.findByStatusOrderByUpdatedAtAsc(FlowPlanVersionStatuses.PUBLISHED))
                .thenReturn(List.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(List.of(root));
        when(flowPlanGraphService.validateAndOrder(List.of(root))).thenReturn(List.of(root));
        when(flowPlanConditionService.referencesAsset(rootDependency(), BIZ_DATE.toLocalDate(), ASSET))
                .thenReturn(true);
        when(triggerHistoryService.findByTriggerKey(anyString()))
                .thenThrow(new IllegalStateException("trigger store unavailable"));

        assertThrows(
                IllegalStateException.class,
                () -> flowPlanEvaluationService.evaluateTriggeredAsset(ASSET, "101", BIZ_DATE));

        verify(flowPlanDecisionMetrics).recordDecision(
                eq(FlowPlanTriggerDecision.FAILED),
                any(Duration.class));
    }

    /**
     * Verify every public FlowPlanTriggerOutcome factory maps its explicit result semantics.
     */
    @Test
    void flowPlanTriggerOutcomeFactoriesExposeDirectResultContracts() {
        FlowPlanVersion version = version(Map.of());

        FlowPlanEvaluationService.FlowPlanTriggerOutcome emitted =
                FlowPlanEvaluationService.FlowPlanTriggerOutcome.emitted(version, "trigger-1", 11L, 71L);
        FlowPlanEvaluationService.FlowPlanTriggerOutcome skipped =
                FlowPlanEvaluationService.FlowPlanTriggerOutcome.skipped(version, "trigger-2", "waiting");
        FlowPlanEvaluationService.FlowPlanTriggerOutcome deduped =
                FlowPlanEvaluationService.FlowPlanTriggerOutcome.deduped(version, "trigger-3", 12L);

        assertTrue(emitted.schedulingIntentEmitted());
        assertEquals(FlowPlanTriggerDecision.EMITTED, emitted.decision());
        assertEquals(71L, emitted.firstTaskInstanceId());
        assertFalse(skipped.schedulingIntentEmitted());
        assertEquals(FlowPlanTriggerDecision.BLOCKED_CONDITION, skipped.decision());
        assertEquals("waiting", skipped.reason());
        assertFalse(deduped.schedulingIntentEmitted());
        assertEquals(FlowPlanTriggerDecision.DEDUPLICATED, deduped.decision());
        assertEquals(12L, deduped.workflowInstanceId());
    }

    /**
     * Build a published FlowPlan version fixture.
     *
     * @param triggerPolicy trigger policy JSON
     * @return version fixture
     */
    private FlowPlanVersion version(Map<String, Object> triggerPolicy) {
        return FlowPlanVersion.builder()
                .id(51L)
                .flowCode("flow.orders")
                .version(4)
                .status(FlowPlanVersionStatuses.PUBLISHED)
                .dependencySpecJson(Map.of())
                .triggerPolicyJson(triggerPolicy)
                .build();
    }

    /**
     * Build one graph node fixture.
     *
     * @param id node id
     * @param code node code
     * @param dependencies direct upstream node codes
     * @param inputDependency external dependency specification
     * @param outputAsset output asset template
     * @return node fixture
     */
    private ScheduleNode node(
            Long id,
            String code,
            List<String> dependencies,
            Map<String, Object> inputDependency,
            String outputAsset) {
        return ScheduleNode.builder()
                .id(id)
                .flowPlanVersionId(51L)
                .nodeCode(code)
                .dependsOnNodes(dependencies)
                .inputDependencySpecJson(inputDependency)
                .outputAssetKey(outputAsset)
                .build();
    }

    /**
     * Build the root dependency map shared by natural-trigger tests.
     *
     * @return root dependency JSON
     */
    private Map<String, Object> rootDependency() {
        return Map.of("type", "SNAPSHOT_ADVANCED", "assetKey", "orders.dt=${bizDate}");
    }
}
