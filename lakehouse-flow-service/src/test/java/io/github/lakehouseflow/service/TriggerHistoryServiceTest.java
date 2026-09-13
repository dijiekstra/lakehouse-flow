package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.TriggerHistoryRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.TriggerHistory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests TriggerHistoryService public audit and lookup methods.
 */
@ExtendWith(MockitoExtension.class)
class TriggerHistoryServiceTest {

    @Mock
    private TriggerHistoryRepository triggerHistoryRepository;

    @InjectMocks
    private TriggerHistoryService triggerHistoryService;

    /**
     * Verify trigger lookup delegates to the repository.
     */
    @Test
    void findByTriggerKeyReturnsRepositoryResult() {
        TriggerHistory history = TriggerHistory.builder().triggerKey("trigger-1").build();
        when(triggerHistoryRepository.findByTriggerKey("trigger-1")).thenReturn(Optional.of(history));

        Optional<TriggerHistory> result = triggerHistoryService.findByTriggerKey("trigger-1");

        assertTrue(result.isPresent());
        assertSame(history, result.get());
    }

    /**
     * Verify workflow trigger audit rows preserve evaluation evidence.
     */
    @Test
    void recordWorkflowTriggerSavesAuditRecord() {
        when(triggerHistoryRepository.findByTriggerKey("trigger-1")).thenReturn(Optional.empty());
        ArgumentCaptor<TriggerHistory> captor = ArgumentCaptor.forClass(TriggerHistory.class);

        triggerHistoryService.recordWorkflowTrigger("trigger-1", "SNAPSHOT_DRIVEN", evaluation(), 11L);

        verify(triggerHistoryRepository).save(captor.capture());
        TriggerHistory saved = captor.getValue();
        assertEquals("trigger-1", saved.getTriggerKey());
        assertEquals(11L, saved.getWorkflowInstanceId());
        assertEquals("TRIGGERED", saved.getDecision());
        assertEquals("100", saved.getEvaluationPayloadJson().get("snapshotId"));
    }

    /**
     * Verify existing workflow trigger keys are not inserted twice.
     */
    @Test
    void recordWorkflowTriggerSkipsExistingAuditRecord() {
        TriggerHistory existing = TriggerHistory.builder().triggerKey("trigger-1").build();
        when(triggerHistoryRepository.findByTriggerKey("trigger-1")).thenReturn(Optional.of(existing));

        triggerHistoryService.recordWorkflowTrigger("trigger-1", "SNAPSHOT_DRIVEN", evaluation(), 11L);

        verify(triggerHistoryRepository, never()).save(any(TriggerHistory.class));
    }

    /**
     * Verify task trigger audit rows preserve task ids.
     */
    @Test
    void recordTaskTriggerSavesAuditRecord() {
        when(triggerHistoryRepository.findByTriggerKey("trigger-1:task")).thenReturn(Optional.empty());
        ArgumentCaptor<TriggerHistory> captor = ArgumentCaptor.forClass(TriggerHistory.class);

        triggerHistoryService.recordTaskTrigger("trigger-1:task", "SNAPSHOT_DRIVEN", evaluation(), 22L);

        verify(triggerHistoryRepository).save(captor.capture());
        TriggerHistory saved = captor.getValue();
        assertEquals("trigger-1:task", saved.getTriggerKey());
        assertEquals(22L, saved.getTaskInstanceId());
        assertEquals("TRIGGERED", saved.getDecision());
    }

    /**
     * Verify skipped triggers are durable audit records.
     */
    @Test
    void recordSkippedTriggerSavesSkippedAuditRecord() {
        when(triggerHistoryRepository.findByTriggerKey("trigger-skip")).thenReturn(Optional.empty());
        ArgumentCaptor<TriggerHistory> captor = ArgumentCaptor.forClass(TriggerHistory.class);

        triggerHistoryService.recordSkippedTrigger(
                "trigger-skip",
                "SNAPSHOT_DRIVEN",
                "paimon.prod.orders",
                "100",
                "waiting input");

        verify(triggerHistoryRepository).save(captor.capture());
        TriggerHistory saved = captor.getValue();
        assertEquals("SKIPPED", saved.getDecision());
        assertEquals("waiting input", saved.getDecisionReason());
    }

    /**
     * Verify read-side trigger history methods delegate to repository queries.
     */
    @Test
    void queryMethodsReturnRepositoryResults() {
        List<TriggerHistory> workflowRows = List.of(TriggerHistory.builder().workflowInstanceId(11L).build());
        List<TriggerHistory> taskRows = List.of(TriggerHistory.builder().taskInstanceId(22L).build());
        List<TriggerHistory> snapshotRows = List.of(TriggerHistory.builder().snapshotId("100").build());
        List<TriggerHistory> assetRows = List.of(TriggerHistory.builder().assetKey("paimon.prod.orders").build());
        when(triggerHistoryRepository.findByWorkflowInstanceId(11L)).thenReturn(workflowRows);
        when(triggerHistoryRepository.findByTaskInstanceId(22L)).thenReturn(taskRows);
        when(triggerHistoryRepository.findBySnapshotIdOrderByCreatedAtDesc("100")).thenReturn(snapshotRows);
        when(triggerHistoryRepository.findByAssetKeyOrderByCreatedAtDesc("paimon.prod.orders")).thenReturn(assetRows);

        assertSame(workflowRows, triggerHistoryService.findWorkflowTriggers(11L));
        assertSame(taskRows, triggerHistoryService.findTaskTriggers(22L));
        assertSame(snapshotRows, triggerHistoryService.findSnapshotTriggers("100"));
        assertSame(assetRows, triggerHistoryService.findAssetTriggers("paimon.prod.orders"));
    }

    /**
     * Build an evaluation result fixture used by trigger audit records.
     */
    private EvaluationResult evaluation() {
        return EvaluationResult.builder()
                .satisfied(true)
                .assetKey("paimon.prod.orders")
                .snapshotId("100")
                .watermark("2026-09-12T01:00:00")
                .eventId("event-100")
                .description("snapshot exists")
                .evaluatedAt(1L)
                .build();
    }
}
