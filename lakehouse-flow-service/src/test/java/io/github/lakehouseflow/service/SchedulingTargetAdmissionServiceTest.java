package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingTargetAdmissionStatuses;
import io.github.lakehouseflow.dao.SchedulingTargetAdmissionRepository;
import io.github.lakehouseflow.model.SchedulingTargetAdmission;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests persistent target and business-date publication admission behavior.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingTargetAdmissionServiceTest {

    private static final String TARGET_ASSET = "paimon.prod.orders.dt=2026-09-13";
    private static final LocalDate BIZ_DATE = LocalDate.of(2026, 9, 13);

    @Mock
    private SchedulingTargetAdmissionRepository schedulingTargetAdmissionRepository;

    @InjectMocks
    private SchedulingTargetAdmissionService schedulingTargetAdmissionService;

    /** Verify an available slot becomes an active lease for the caller. */
    @Test
    void acquireClaimsAvailableTargetDateSlot() {
        SchedulingTargetAdmission admission = availableAdmission();
        when(schedulingTargetAdmissionRepository.findForUpdate(TARGET_ASSET, BIZ_DATE))
                .thenReturn(Optional.of(admission));
        when(schedulingTargetAdmissionRepository.save(admission)).thenReturn(admission);

        SchedulingTargetAdmissionService.AdmissionDecision decision = acquire(22L, "task-instance:22");

        assertTrue(decision.admitted());
        assertFalse(decision.reclaimedExpiredAdmission());
        assertEquals(22L, decision.holderTaskInstanceId());
        assertEquals(SchedulingTargetAdmissionStatuses.ACTIVE, admission.getStatus());
        assertEquals("SNAPSHOT", admission.getHolderTriggerType());
        assertTrue(admission.getExpiresAt().isAfter(admission.getAcquiredAt()));
        verify(schedulingTargetAdmissionRepository).ensureSlot(TARGET_ASSET, BIZ_DATE);
    }

    /** Verify a live holder blocks another normal or action-owned publication. */
    @Test
    void acquireRejectsDifferentHolderBeforeLeaseExpiry() {
        SchedulingTargetAdmission admission = activeAdmission(21L, "task-instance:21");
        admission.setExpiresAt(LocalDateTime.now().plusHours(1));
        when(schedulingTargetAdmissionRepository.findForUpdate(TARGET_ASSET, BIZ_DATE))
                .thenReturn(Optional.of(admission));

        SchedulingTargetAdmissionService.AdmissionDecision decision = acquire(22L, "task-instance:22");

        assertFalse(decision.admitted());
        assertEquals(21L, decision.holderTaskInstanceId());
        assertEquals("task-instance:21", decision.holderIntentKey());
        verify(schedulingTargetAdmissionRepository, never()).save(any());
    }

    /** Verify an elapsed scheduler lease can be reclaimed by a newer decision. */
    @Test
    void acquireReclaimsExpiredHolder() {
        SchedulingTargetAdmission admission = activeAdmission(21L, "task-instance:21");
        admission.setExpiresAt(LocalDateTime.now().minusSeconds(1));
        when(schedulingTargetAdmissionRepository.findForUpdate(TARGET_ASSET, BIZ_DATE))
                .thenReturn(Optional.of(admission));
        when(schedulingTargetAdmissionRepository.save(admission)).thenReturn(admission);

        SchedulingTargetAdmissionService.AdmissionDecision decision = acquire(22L, "task-instance:22");

        assertTrue(decision.admitted());
        assertTrue(decision.reclaimedExpiredAdmission());
        assertEquals(22L, admission.getHolderTaskInstanceId());
        assertEquals("task-instance:22", admission.getHolderIntentKey());
    }

    /** Verify the same immutable publication may idempotently re-enter its slot. */
    @Test
    void acquireAllowsSameHolderIdempotently() {
        SchedulingTargetAdmission admission = activeAdmission(22L, "task-instance:22");
        admission.setExpiresAt(LocalDateTime.now().plusHours(1));
        when(schedulingTargetAdmissionRepository.findForUpdate(TARGET_ASSET, BIZ_DATE))
                .thenReturn(Optional.of(admission));

        SchedulingTargetAdmissionService.AdmissionDecision decision = acquire(22L, "task-instance:22");

        assertTrue(decision.admitted());
        assertFalse(decision.reclaimedExpiredAdmission());
        verify(schedulingTargetAdmissionRepository, never()).save(any());
    }

    /** Verify missing expiry evidence fails closed while another holder is active. */
    @Test
    void acquireDoesNotReclaimActiveHolderWithoutExpiry() {
        SchedulingTargetAdmission admission = activeAdmission(21L, "task-instance:21");
        admission.setExpiresAt(null);
        when(schedulingTargetAdmissionRepository.findForUpdate(TARGET_ASSET, BIZ_DATE))
                .thenReturn(Optional.of(admission));

        assertFalse(acquire(22L, "task-instance:22").admitted());
        verify(schedulingTargetAdmissionRepository, never()).save(any());
    }

    /** Verify snapshot terminal evidence releases a matching active holder. */
    @Test
    void releaseMakesMatchingHolderAvailable() {
        SchedulingTargetAdmission admission = activeAdmission(22L, "task-instance:22");
        when(schedulingTargetAdmissionRepository.findForUpdate(TARGET_ASSET, BIZ_DATE))
                .thenReturn(Optional.of(admission));
        when(schedulingTargetAdmissionRepository.save(admission)).thenReturn(admission);

        boolean released = schedulingTargetAdmissionService.release(
                TARGET_ASSET,
                BIZ_DATE,
                22L,
                "SNAPSHOT_CONFIRMED");

        assertTrue(released);
        assertEquals(SchedulingTargetAdmissionStatuses.AVAILABLE, admission.getStatus());
        assertEquals("SNAPSHOT_CONFIRMED", admission.getReleaseReason());
        assertTrue(admission.getReleasedAt() != null);
    }

    /** Verify stale confirmation cannot release a replacement holder's lease. */
    @Test
    void releaseIgnoresNonHolderTask() {
        SchedulingTargetAdmission admission = activeAdmission(23L, "task-instance:23");
        when(schedulingTargetAdmissionRepository.findForUpdate(TARGET_ASSET, BIZ_DATE))
                .thenReturn(Optional.of(admission));

        assertFalse(schedulingTargetAdmissionService.release(
                TARGET_ASSET,
                BIZ_DATE,
                22L,
                "SNAPSHOT_CONFIRMED"));
        verify(schedulingTargetAdmissionRepository, never()).save(any());
    }

    /** Verify malformed acquisition input is rejected before persistence access. */
    @Test
    void acquireRejectsInvalidLeaseInput() {
        assertThrows(IllegalArgumentException.class, () -> schedulingTargetAdmissionService.acquire(
                TARGET_ASSET,
                BIZ_DATE,
                22L,
                "task-instance:22",
                "SNAPSHOT",
                Duration.ZERO));
        verify(schedulingTargetAdmissionRepository, never()).ensureSlot(any(), any());
    }

    /** Acquire the shared fixture slot for one test holder. */
    private SchedulingTargetAdmissionService.AdmissionDecision acquire(Long taskId, String intentKey) {
        return schedulingTargetAdmissionService.acquire(
                TARGET_ASSET,
                BIZ_DATE,
                taskId,
                intentKey,
                "SNAPSHOT",
                Duration.ofHours(1));
    }

    /** Build one reusable available target-date slot. */
    private SchedulingTargetAdmission availableAdmission() {
        return SchedulingTargetAdmission.builder()
                .id(1L)
                .targetAssetKey(TARGET_ASSET)
                .bizDate(BIZ_DATE)
                .status(SchedulingTargetAdmissionStatuses.AVAILABLE)
                .build();
    }

    /** Build one active slot held by a specified immutable publication. */
    private SchedulingTargetAdmission activeAdmission(Long taskId, String intentKey) {
        SchedulingTargetAdmission admission = availableAdmission();
        admission.setStatus(SchedulingTargetAdmissionStatuses.ACTIVE);
        admission.setHolderTaskInstanceId(taskId);
        admission.setHolderIntentKey(intentKey);
        admission.setAcquiredAt(LocalDateTime.now().minusMinutes(5));
        return admission;
    }
}
