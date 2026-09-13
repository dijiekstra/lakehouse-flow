package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.AssetDependencyRepository;
import io.github.lakehouseflow.model.AssetDependency;
import io.github.lakehouseflow.model.DependencyEvaluationOutcome;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.TriggerHistory;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests dependency evaluation and scheduling-intent emission.
 */
@ExtendWith(MockitoExtension.class)
class DependencyEvaluationServiceTest {

    private static final String INPUT_ASSET = "paimon.prod.ods_orders";
    private static final String TARGET_ASSET = "paimon.prod.dwd_orders";
    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 12, 0, 0);

    @Mock
    private AssetDependencyRepository assetDependencyRepository;

    @Mock
    private ConditionEvaluator conditionEvaluator;

    @Mock
    private WorkflowInstanceService workflowInstanceService;

    @Mock
    private TaskInstanceService taskInstanceService;

    @Mock
    private TriggerHistoryService triggerHistoryService;

    @InjectMocks
    private DependencyEvaluationService dependencyEvaluationService;

    /**
     * Verify that satisfied dependencies create scheduling-side workflow and task records.
     */
    @Test
    void emitsSchedulingIntentWhenDependencyConditionsAreSatisfied() {
        AssetDependency dependency = dependency();
        WorkflowInstance workflow = workflowInstance();
        TaskInstance task = taskInstance(SchedulingStates.READY_TO_SCHEDULE);

        when(triggerHistoryService.findByTriggerKey(anyString())).thenReturn(Optional.empty());
        when(conditionEvaluator.evaluateCondition("SNAPSHOT_EXISTS", INPUT_ASSET, null))
                .thenReturn(EvaluationResult.builder()
                        .satisfied(true)
                        .assetKey(INPUT_ASSET)
                        .snapshotId("100")
                        .description("Snapshot 100 exists")
                        .build());
        when(workflowInstanceService.createInstance(
                eq("flow.orders"),
                eq(2),
                eq(BIZ_DATE),
                eq("SNAPSHOT_DRIVEN"),
                anyString(),
                anyString())).thenReturn(workflow);
        when(taskInstanceService.createInstance(11L, "node.dwd_orders", 3, BIZ_DATE, TARGET_ASSET))
                .thenReturn(task);

        DependencyEvaluationOutcome outcome = dependencyEvaluationService.evaluateAndEmit(
                dependency,
                INPUT_ASSET,
                "100",
                BIZ_DATE);

        assertTrue(outcome.dependencySatisfied());
        assertTrue(outcome.schedulingIntentEmitted());
        assertEquals(11L, outcome.workflowInstanceId());
        assertEquals(22L, outcome.taskInstanceId());
        assertEquals(SchedulingStates.READY_TO_SCHEDULE, outcome.resultingState());
        verify(workflowInstanceService).markSchedulable(11L);
        verify(taskInstanceService).markSchedulable(22L);
        verify(triggerHistoryService).recordWorkflowTrigger(
                eq(outcome.triggerKey()),
                eq("SNAPSHOT_DRIVEN"),
                any(EvaluationResult.class),
                eq(11L));
        verify(triggerHistoryService).recordTaskTrigger(
                eq(outcome.triggerKey() + ":task"),
                eq("SNAPSHOT_DRIVEN"),
                any(EvaluationResult.class),
                eq(22L));
    }

    /**
     * Verify that unsatisfied dependencies are audited without emitting scheduling intent.
     */
    @Test
    void recordsSkippedTriggerWhenDependencyConditionsAreNotSatisfied() {
        AssetDependency dependency = dependency();
        when(triggerHistoryService.findByTriggerKey(anyString())).thenReturn(Optional.empty());
        when(conditionEvaluator.evaluateCondition("SNAPSHOT_EXISTS", INPUT_ASSET, null))
                .thenReturn(EvaluationResult.unsatisfied("Asset has no snapshot"));

        DependencyEvaluationOutcome outcome = dependencyEvaluationService.evaluateAndEmit(
                dependency,
                INPUT_ASSET,
                "100",
                BIZ_DATE);

        assertFalse(outcome.dependencySatisfied());
        assertFalse(outcome.schedulingIntentEmitted());
        assertEquals("Asset has no snapshot", outcome.reason());
        verify(triggerHistoryService).recordSkippedTrigger(
                eq(outcome.triggerKey()),
                eq("SNAPSHOT_DRIVEN"),
                eq(INPUT_ASSET),
                eq("100"),
                eq("Asset has no snapshot"));
        verify(workflowInstanceService, never()).createInstance(anyString(), any(), any(), anyString(), anyString(), anyString());
    }

    /**
     * Verify trigger-key idempotency prevents duplicate scheduling decisions.
     */
    @Test
    void skipsEvaluationWhenTriggerKeyWasAlreadyRecorded() {
        AssetDependency dependency = dependency();
        TriggerHistory existing = TriggerHistory.builder()
                .triggerKey("snapshot:7:already")
                .workflowInstanceId(11L)
                .taskInstanceId(22L)
                .build();
        when(triggerHistoryService.findByTriggerKey(anyString())).thenReturn(Optional.of(existing));

        DependencyEvaluationOutcome outcome = dependencyEvaluationService.evaluateAndEmit(
                dependency,
                INPUT_ASSET,
                "100",
                BIZ_DATE);

        assertTrue(outcome.dependencySatisfied());
        assertFalse(outcome.schedulingIntentEmitted());
        assertEquals("DEDUPED", outcome.resultingState());
        assertEquals(11L, outcome.workflowInstanceId());
        verify(conditionEvaluator, never()).evaluateCondition(anyString(), anyString(), any());
    }

    /**
     * Verify all dependencies registered to a changed asset are evaluated.
     */
    @Test
    void evaluatesAllDependenciesForChangedAsset() {
        AssetDependency dependency = dependency();
        when(assetDependencyRepository.findByAssetKeyAndEnabledTrueOrderByCreatedAtAsc(INPUT_ASSET))
                .thenReturn(List.of(dependency));
        when(triggerHistoryService.findByTriggerKey(anyString())).thenReturn(Optional.empty());
        when(conditionEvaluator.evaluateCondition("SNAPSHOT_EXISTS", INPUT_ASSET, null))
                .thenReturn(EvaluationResult.unsatisfied("Asset has no snapshot"));

        List<DependencyEvaluationOutcome> outcomes = dependencyEvaluationService.evaluateTriggeredAsset(
                INPUT_ASSET,
                "100",
                BIZ_DATE);

        assertEquals(1, outcomes.size());
        assertFalse(outcomes.get(0).dependencySatisfied());
    }

    /**
     * Build a dependency fixture using the normalized dependency DSL.
     */
    private AssetDependency dependency() {
        Map<String, Object> condition = new LinkedHashMap<>();
        condition.put("type", "SNAPSHOT_EXISTS");
        condition.put("assetKey", INPUT_ASSET);

        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("operator", "AND");
        spec.put("workflowVersion", 2);
        spec.put("taskVersion", 3);
        spec.put("targetAssetKey", TARGET_ASSET);
        spec.put("conditions", List.of(condition));

        return AssetDependency.builder()
                .id(7L)
                .assetKey(INPUT_ASSET)
                .workflowCode("flow.orders")
                .taskCode("node.dwd_orders")
                .dependencyConditions(spec)
                .enabled(true)
                .build();
    }

    /**
     * Build a workflow instance fixture for emitted scheduling decisions.
     */
    private WorkflowInstance workflowInstance() {
        return WorkflowInstance.builder()
                .id(11L)
                .workflowCode("flow.orders")
                .workflowVersion(2)
                .state(SchedulingStates.CREATED)
                .build();
    }

    /**
     * Build a task instance fixture in the supplied scheduling state.
     */
    private TaskInstance taskInstance(String state) {
        return TaskInstance.builder()
                .id(22L)
                .workflowInstanceId(11L)
                .taskCode("node.dwd_orders")
                .taskVersion(3)
                .state(state)
                .build();
    }
}
