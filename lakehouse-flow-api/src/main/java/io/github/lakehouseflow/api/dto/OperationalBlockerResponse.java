package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * API response for one scheduler, transport, or snapshot-evidence blocker.
 *
 * @param blockerType stable blocker category
 * @param subjectType scheduler-owned record family
 * @param subjectId scheduler-owned record id
 * @param subjectKey stable task, batch-item, or intent identity
 * @param flowCode owning or affected Flow when known
 * @param taskCode task or node code when applicable
 * @param writerJobKey platform writer key when applicable
 * @param targetAssetKey target asset related to the blocker
 * @param bizDate business date when applicable
 * @param schedulingState scheduler-side state when applicable
 * @param deliveryStatus transport-only state when applicable
 * @param sourceHealth independent source-health result when applicable
 * @param reason scheduler, transport, or source evidence detail
 * @param observedAt latest evidence update time
 */
public record OperationalBlockerResponse(
        String blockerType,
        String subjectType,
        Long subjectId,
        String subjectKey,
        String flowCode,
        String taskCode,
        String writerJobKey,
        String targetAssetKey,
        LocalDateTime bizDate,
        String schedulingState,
        String deliveryStatus,
        String sourceHealth,
        String reason,
        LocalDateTime observedAt) {
}
