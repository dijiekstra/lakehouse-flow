package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.InputSnapshotEvidence;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests mixed stream/batch parent evidence and immutable input-vector construction.
 */
@ExtendWith(MockitoExtension.class)
class InputSnapshotEvidenceServiceTest {

    private static final LocalDate BIZ_DATE = LocalDate.of(2026, 9, 13);

    @Mock
    private FlowPlanVersionRepository flowPlanVersionRepository;

    @Mock
    private ScheduleNodeRepository scheduleNodeRepository;

    @Mock
    private TaskInstanceRepository taskInstanceRepository;

    @Mock
    private AssetStateRepository assetStateRepository;

    @Mock
    private FlowPlanConditionService flowPlanConditionService;

    @Mock
    private SchedulingTemplateResolver schedulingTemplateResolver;

    @InjectMocks
    private InputSnapshotEvidenceService inputSnapshotEvidenceService;

    private FlowPlanVersion version;
    private ScheduleNode streamParent;
    private ScheduleNode batchParent;
    private ScheduleNode join;
    private TaskInstance batchParentTask;
    private TaskInstance joinTask;

    /** Build one mixed graph and same-instance batch task set. */
    @BeforeEach
    void setUpGraph() {
        version = FlowPlanVersion.builder().id(51L).dependencySpecJson(Map.of()).build();
        streamParent = node(61L, "orders-stream", ScheduleNodeProcessingModes.STREAMING, List.of(),
                "lake.ods.orders.dt=${bizDate}");
        batchParent = node(62L, "payments-daily", ScheduleNodeProcessingModes.BATCH, List.of(),
                "lake.ods.payments.dt=${bizDate}");
        join = node(63L, "orders-join", ScheduleNodeProcessingModes.BATCH,
                List.of("orders-stream", "payments-daily"), "lake.dwd.orders.dt=${bizDate}");
        batchParentTask = task(72L, 62L, "payments-daily", SchedulingStates.SNAPSHOT_CONFIRMED);
        batchParentTask.setTargetAssetKey("lake.ods.payments.dt=2026-09-13");
        batchParentTask.setObservedSnapshotId("433");
        batchParentTask.setLastSnapshotCheckAt(LocalDateTime.of(2026, 9, 13, 2, 0));
        joinTask = task(73L, 63L, "orders-join", SchedulingStates.WAITING_SNAPSHOT);
    }

    /** Verify a stream asset and same-instance batch task jointly release a downstream batch node. */
    @Test
    void evaluateBuildsMixedParentAndExternalEvidenceVector() {
        Map<String, Object> externalSpec = Map.of(
                "type", "SNAPSHOT_ADVANCED",
                "assetKey", "lake.ref.calendar.dt=${bizDate}");
        join.setInputDependencySpecJson(externalSpec);
        stubDefinition(List.of(streamParent, batchParent, join), List.of(batchParentTask, joinTask));
        when(schedulingTemplateResolver.resolve("lake.ods.orders.dt=${bizDate}", BIZ_DATE))
                .thenReturn("lake.ods.orders.dt=2026-09-13");
        AssetState streamState = state("lake.ods.orders.dt=2026-09-13", "812", "2026-09-13T23:59:59");
        when(assetStateRepository.findByAssetKey(streamState.getAssetKey()))
                .thenReturn(Optional.of(streamState));
        EvaluationResult externalEvidence = EvaluationResult.builder()
                .satisfied(true)
                .assetKey("lake.ref.calendar.dt=2026-09-13")
                .snapshotId("17")
                .build();
        when(flowPlanConditionService.evaluateWithEvidence(externalSpec, BIZ_DATE))
                .thenReturn(new FlowPlanConditionService.DependencyEvaluation(
                        EvaluationResult.satisfied(),
                        List.of(externalEvidence)));
        when(assetStateRepository.findByAssetKey("lake.ref.calendar.dt=2026-09-13"))
                .thenReturn(Optional.of(state("lake.ref.calendar.dt=2026-09-13", "17", null)));

        InputSnapshotEvidenceService.InputEvidenceEvaluation result =
                inputSnapshotEvidenceService.evaluate(joinTask);

        assertTrue(result.satisfied());
        assertEquals(ScheduleNodeProcessingModes.BATCH, result.processingMode());
        assertEquals(3, result.evidence().size());
        InputSnapshotEvidence streamEvidence = result.evidence().get(0);
        assertEquals("ASSET_SNAPSHOT", streamEvidence.evidenceSource());
        assertNull(streamEvidence.upstreamTaskInstanceId());
        assertEquals("812", streamEvidence.snapshotId());
        InputSnapshotEvidence batchEvidence = result.evidence().get(1);
        assertEquals("SCHEDULE_INSTANCE_OUTPUT", batchEvidence.evidenceSource());
        assertEquals(72L, batchEvidence.upstreamTaskInstanceId());
        assertEquals("EXTERNAL_ASSET", result.evidence().get(2).evidenceSource());
    }

    /** Verify an action-owned streaming parent must confirm its own replay task before release. */
    @Test
    void evaluateRequiresActionOwnedStreamingParentTaskConfirmation() {
        TaskInstance streamTask = task(71L, 61L, "orders-stream", SchedulingStates.SCHEDULED);
        stubDefinition(List.of(streamParent, batchParent, join), List.of(streamTask, batchParentTask, joinTask));

        InputSnapshotEvidenceService.InputEvidenceEvaluation result =
                inputSnapshotEvidenceService.evaluate(joinTask);

        assertFalse(result.satisfied());
        assertTrue(result.waitingReason().contains("orders-stream"));
        verify(assetStateRepository, never()).findByAssetKey("lake.ods.orders.dt=2026-09-13");
    }

    /** Verify a batch parent cannot be replaced by unrelated historical AssetState evidence. */
    @Test
    void evaluateBlocksMissingSameInstanceBatchParent() {
        join.setDependsOnNodes(List.of("payments-daily"));
        stubDefinition(List.of(batchParent, join), List.of(joinTask));

        InputSnapshotEvidenceService.InputEvidenceEvaluation result =
                inputSnapshotEvidenceService.evaluate(joinTask);

        assertFalse(result.satisfied());
        assertTrue(result.waitingReason().contains("same-instance batch parent"));
    }

    /** Verify a selected backfill or rerun entry intentionally starts with an empty parent vector. */
    @Test
    void evaluateHonorsPersistedActionEntryBypass() {
        joinTask.setParentDependencyBypassed(true);
        stubDefinition(List.of(streamParent, batchParent, join), List.of(joinTask));

        InputSnapshotEvidenceService.InputEvidenceEvaluation result =
                inputSnapshotEvidenceService.evaluate(joinTask);

        assertTrue(result.satisfied());
        assertTrue(result.evidence().isEmpty());
        verify(taskInstanceRepository, never()).findByWorkflowInstanceIdOrderByCreatedAtAsc(11L);
    }

    /** Verify legacy task intents remain bounded batch instructions with no graph vector. */
    @Test
    void evaluateKeepsLegacyTaskCompatibility() {
        TaskInstance legacy = TaskInstance.builder().id(90L).build();

        InputSnapshotEvidenceService.InputEvidenceEvaluation result =
                inputSnapshotEvidenceService.evaluate(legacy);

        assertTrue(result.satisfied());
        assertEquals(ScheduleNodeProcessingModes.BATCH, result.processingMode());
        assertTrue(result.evidence().isEmpty());
    }

    /** Stub immutable graph definition and same-instance task rows. */
    private void stubDefinition(List<ScheduleNode> nodes, List<TaskInstance> tasks) {
        when(flowPlanVersionRepository.findById(51L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findByFlowPlanVersionIdOrderBySortOrderAscCreatedAtAsc(51L))
                .thenReturn(nodes);
        if (!joinTask.isParentDependencyBypassed()) {
            when(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(11L)).thenReturn(tasks);
        }
    }

    /** Build one graph node fixture. */
    private ScheduleNode node(
            Long id,
            String code,
            String processingMode,
            List<String> dependencies,
            String outputAssetKey) {
        return ScheduleNode.builder()
                .id(id)
                .flowPlanVersionId(51L)
                .nodeCode(code)
                .processingMode(processingMode)
                .dependsOnNodes(dependencies)
                .inputDependencySpecJson(Map.of())
                .outputAssetKey(outputAssetKey)
                .build();
    }

    /** Build one same-workflow task fixture. */
    private TaskInstance task(Long id, Long nodeId, String code, String state) {
        return TaskInstance.builder()
                .id(id)
                .workflowInstanceId(11L)
                .flowPlanVersionId(51L)
                .scheduleNodeId(nodeId)
                .taskCode(code)
                .bizDate(BIZ_DATE.atStartOfDay())
                .state(state)
                .build();
    }

    /** Build one business-data AssetState fixture. */
    private AssetState state(String assetKey, String snapshotId, String watermark) {
        return AssetState.builder()
                .assetKey(assetKey)
                .latestDataSnapshotId(snapshotId)
                .latestDataWatermark(watermark == null ? null : LocalDateTime.parse(watermark))
                .updatedAt(LocalDateTime.of(2026, 9, 14, 1, 0))
                .build();
    }
}
