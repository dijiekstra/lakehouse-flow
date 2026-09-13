package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingTargetAdmissionStatuses;
import io.github.lakehouseflow.dao.SchedulingTargetAdmissionRepository;
import io.github.lakehouseflow.model.SchedulingTargetAdmission;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Serializes scheduling-intent publication by managed target and business date.
 *
 * The lease is held from baseline capture until attributable snapshot
 * confirmation or confirmation timeout. Reclaiming an expired lease authorizes
 * a new scheduling instruction; it does not assert that an earlier downstream
 * process stopped running.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class SchedulingTargetAdmissionService {

    private final SchedulingTargetAdmissionRepository schedulingTargetAdmissionRepository;

    /**
     * Result of one target and date admission attempt.
     *
     * @param admitted whether the caller owns the publication slot
     * @param reclaimedExpiredAdmission whether an expired previous lease was replaced
     * @param holderTaskInstanceId current holder task id
     * @param holderIntentKey current holder intent key
     * @param expiresAt current holder lease expiry
     */
    public record AdmissionDecision(
            boolean admitted,
            boolean reclaimedExpiredAdmission,
            Long holderTaskInstanceId,
            String holderIntentKey,
            LocalDateTime expiresAt) {
    }

    /**
     * Acquire or idempotently re-enter one target and business-date slot.
     *
     * @param targetAssetKey managed target asset key
     * @param bizDate business date represented by the scheduling decision
     * @param taskInstanceId task scheduling decision requesting publication
     * @param intentKey immutable intent attribution key
     * @param triggerType scheduling trigger mode
     * @param leaseDuration maximum scheduler-side confirmation window
     * @return admission decision and current holder evidence
     */
    public AdmissionDecision acquire(
            String targetAssetKey,
            LocalDate bizDate,
            Long taskInstanceId,
            String intentKey,
            String triggerType,
            Duration leaseDuration) {
        String normalizedTarget = requireText(targetAssetKey, "targetAssetKey");
        LocalDate normalizedBizDate = requireBizDate(bizDate);
        Long normalizedTaskId = requireTaskInstanceId(taskInstanceId);
        String normalizedIntentKey = requireText(intentKey, "intentKey");
        String normalizedTriggerType = requireText(triggerType, "triggerType");
        Duration normalizedLease = requireLeaseDuration(leaseDuration);

        schedulingTargetAdmissionRepository.ensureSlot(normalizedTarget, normalizedBizDate);
        SchedulingTargetAdmission admission = schedulingTargetAdmissionRepository
                .findForUpdate(normalizedTarget, normalizedBizDate)
                .orElseThrow(() -> new IllegalStateException(
                        "Target admission slot initialization failed: "
                                + normalizedTarget + "@" + normalizedBizDate));
        LocalDateTime now = LocalDateTime.now();

        if (isActive(admission) && !isExpired(admission, now)) {
            if (admission.getExpiresAt() != null
                    && isSameHolder(admission, normalizedTaskId, normalizedIntentKey)) {
                return decision(true, false, admission);
            }
            return decision(false, false, admission);
        }
        if (!isActive(admission)
                && !SchedulingTargetAdmissionStatuses.AVAILABLE.equals(admission.getStatus())) {
            throw new IllegalStateException("Unsupported target admission status: " + admission.getStatus());
        }

        boolean reclaimedExpiredAdmission = isActive(admission);
        admission.setHolderTaskInstanceId(normalizedTaskId);
        admission.setHolderIntentKey(normalizedIntentKey);
        admission.setHolderTriggerType(normalizedTriggerType);
        admission.setStatus(SchedulingTargetAdmissionStatuses.ACTIVE);
        admission.setAcquiredAt(now);
        admission.setExpiresAt(now.plus(normalizedLease));
        admission.setReleasedAt(null);
        admission.setReleaseReason(null);
        SchedulingTargetAdmission saved = schedulingTargetAdmissionRepository.save(admission);
        return decision(true, reclaimedExpiredAdmission, saved);
    }

    /**
     * Release a slot only when the expected task still owns it.
     *
     * A late confirmation from an expired holder must not release a newer
     * holder's lease for the same target and date.
     *
     * @param targetAssetKey managed target asset key
     * @param bizDate protected business date
     * @param taskInstanceId expected current holder task id
     * @param reason scheduler-side terminal reason
     * @return true when an active matching holder was released
     */
    public boolean release(
            String targetAssetKey,
            LocalDate bizDate,
            Long taskInstanceId,
            String reason) {
        String normalizedTarget = requireText(targetAssetKey, "targetAssetKey");
        LocalDate normalizedBizDate = requireBizDate(bizDate);
        Long normalizedTaskId = requireTaskInstanceId(taskInstanceId);
        String normalizedReason = requireText(reason, "reason");

        return schedulingTargetAdmissionRepository.findForUpdate(normalizedTarget, normalizedBizDate)
                .filter(this::isActive)
                .filter(admission -> normalizedTaskId.equals(admission.getHolderTaskInstanceId()))
                .map(admission -> {
                    admission.setStatus(SchedulingTargetAdmissionStatuses.AVAILABLE);
                    admission.setReleasedAt(LocalDateTime.now());
                    admission.setReleaseReason(normalizedReason);
                    schedulingTargetAdmissionRepository.save(admission);
                    return true;
                })
                .orElse(false);
    }

    /** Build an immutable decision from the persisted current holder. */
    private AdmissionDecision decision(
            boolean admitted,
            boolean reclaimedExpiredAdmission,
            SchedulingTargetAdmission admission) {
        return new AdmissionDecision(
                admitted,
                reclaimedExpiredAdmission,
                admission.getHolderTaskInstanceId(),
                admission.getHolderIntentKey(),
                admission.getExpiresAt());
    }

    /** Check whether a slot currently has an active publication lease. */
    private boolean isActive(SchedulingTargetAdmission admission) {
        return SchedulingTargetAdmissionStatuses.ACTIVE.equals(admission.getStatus());
    }

    /** Check whether an active slot belongs to the same idempotent publication. */
    private boolean isSameHolder(
            SchedulingTargetAdmission admission,
            Long taskInstanceId,
            String intentKey) {
        return taskInstanceId.equals(admission.getHolderTaskInstanceId())
                && intentKey.equals(admission.getHolderIntentKey());
    }

    /** Check whether the current holder's scheduler-side lease elapsed. */
    private boolean isExpired(SchedulingTargetAdmission admission, LocalDateTime now) {
        return admission.getExpiresAt() != null && !now.isBefore(admission.getExpiresAt());
    }

    /** Require and normalize a nonblank text field. */
    private String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /** Require a business date for the target admission key. */
    private LocalDate requireBizDate(LocalDate bizDate) {
        if (bizDate == null) {
            throw new IllegalArgumentException("bizDate must not be null");
        }
        return bizDate;
    }

    /** Require a positive task scheduling decision identifier. */
    private Long requireTaskInstanceId(Long taskInstanceId) {
        if (taskInstanceId == null || taskInstanceId <= 0) {
            throw new IllegalArgumentException("taskInstanceId must be positive");
        }
        return taskInstanceId;
    }

    /** Require a positive scheduler-side lease duration. */
    private Duration requireLeaseDuration(Duration leaseDuration) {
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        return leaseDuration;
    }
}
