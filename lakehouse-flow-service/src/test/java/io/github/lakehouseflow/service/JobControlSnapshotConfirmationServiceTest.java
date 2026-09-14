package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.JobControlIntentContract;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.model.EvaluationResult;
import io.github.lakehouseflow.model.JobControlIntent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests writer-generation confirmation from snapshots and independent source health.
 */
@ExtendWith(MockitoExtension.class)
class JobControlSnapshotConfirmationServiceTest {

    @Mock
    private JobControlIntentRepository intentRepository;

    @Mock
    private SnapshotEvidenceService snapshotEvidenceService;

    @Mock
    private SnapshotSourceHealthService snapshotSourceHealthService;

    @InjectMocks
    private JobControlSnapshotConfirmationService service;

    /** Preserve entity identity while testing confirmation mutations. */
    @BeforeEach
    void saveEntities() {
        org.mockito.Mockito.lenient().when(intentRepository.save(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    /** Verify an attributable business snapshot confirms the controlled writer epoch. */
    @Test
    void checkIntentConfirmsWriterSnapshot() {
        JobControlIntent intent = intent(LocalDateTime.now().plusMinutes(5));
        when(intentRepository.findByIdForUpdate(11L)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateJobControlProgress(intent)).thenReturn(evidence(true, "812"));

        JobControlSnapshotConfirmationService.JobControlConfirmationResult result = service.checkIntent(11L);

        assertEquals(JobControlIntentContract.SNAPSHOT_CONFIRMED, result.snapshotResult());
        assertEquals("812", result.observedSnapshotId());
        verify(snapshotSourceHealthService, never()).evaluateTimeoutEvidence(any(), any());
    }

    /** Verify a healthy source can conclude not-advanced after the observation deadline. */
    @Test
    void checkIntentMarksNotAdvancedOnlyWithHealthySource() {
        JobControlIntent intent = intent(LocalDateTime.now().minusMinutes(1));
        when(intentRepository.findByIdForUpdate(11L)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateJobControlProgress(intent)).thenReturn(evidence(false, "811"));
        when(snapshotSourceHealthService.evaluateTimeoutEvidence(
                "paimon.ods.orders", intent.getConfirmationDeadline()))
                .thenReturn(new SnapshotSourceHealthService.SourceHealthDecision(
                        "paimon.ods.orders", "HEALTHY", true, "source caught up", LocalDateTime.now()));

        JobControlSnapshotConfirmationService.JobControlConfirmationResult result = service.checkIntent(11L);

        assertEquals(JobControlIntentContract.SNAPSHOT_NOT_ADVANCED, result.snapshotResult());
        assertEquals("HEALTHY", result.sourceHealth());
    }

    /** Verify incomplete source evidence keeps the snapshot result waiting after timeout. */
    @Test
    void checkIntentKeepsWaitingWhenSourceBlocked() {
        JobControlIntent intent = intent(LocalDateTime.now().minusMinutes(1));
        when(intentRepository.findByIdForUpdate(11L)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateJobControlProgress(intent)).thenReturn(evidence(false, "811"));
        when(snapshotSourceHealthService.evaluateTimeoutEvidence(
                "paimon.ods.orders", intent.getConfirmationDeadline()))
                .thenReturn(new SnapshotSourceHealthService.SourceHealthDecision(
                        "paimon.ods.orders", "SOURCE_BLOCKED", false, "offset gap", LocalDateTime.now()));

        JobControlSnapshotConfirmationService.JobControlConfirmationResult result = service.checkIntent(11L);

        assertEquals(JobControlIntentContract.WAITING, result.snapshotResult());
        assertTrue(result.waitingReason().contains("SOURCE_BLOCKED"));
    }

    /** Verify the batch scanner directly checks every waiting control intent. */
    @Test
    void checkWaitingIntentsChecksRepositoryBatch() {
        JobControlIntent intent = intent(LocalDateTime.now().plusMinutes(5));
        when(intentRepository.findBySnapshotResultOrderByCreatedAtAsc(JobControlIntentContract.WAITING))
                .thenReturn(List.of(intent));
        when(intentRepository.findByIdForUpdate(11L)).thenReturn(Optional.of(intent));
        when(snapshotEvidenceService.evaluateJobControlProgress(intent)).thenReturn(evidence(false, "811"));

        List<JobControlSnapshotConfirmationService.JobControlConfirmationResult> results =
                service.checkWaitingIntents();

        assertEquals(1, results.size());
        assertEquals(JobControlIntentContract.WAITING, results.get(0).snapshotResult());
    }

    /** Build one waiting job-control intent fixture. */
    private JobControlIntent intent(LocalDateTime confirmationDeadline) {
        return JobControlIntent.builder()
                .id(11L)
                .intentKey("job-control:writer.orders:7")
                .writerJobKey("writer.orders")
                .writerEpoch(7L)
                .tableAssetKey("paimon.ods.orders")
                .baselineSnapshotId("811")
                .confirmationDeadline(confirmationDeadline)
                .snapshotResult(JobControlIntentContract.WAITING)
                .build();
    }

    /** Build one snapshot evidence result. */
    private EvaluationResult evidence(boolean satisfied, String snapshotId) {
        return EvaluationResult.builder()
                .satisfied(satisfied)
                .assetKey("paimon.ods.orders")
                .snapshotId(snapshotId)
                .waitingReason(satisfied ? null : "waiting for writer snapshot")
                .build();
    }
}
