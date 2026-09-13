package io.github.lakehouseflow.api.dto;

import java.util.List;
import java.util.Map;

/**
 * Complete scheduler-side action audit response.
 *
 * @param action action summary
 * @param targetAssetKey action-level target asset when applicable
 * @param targetSnapshotId action-level target snapshot when applicable
 * @param requestPayload structured action-specific request payload
 * @param backfillBatch related batch evidence when applicable
 * @param snapshotEvidence task-level target snapshot evidence
 */
public record SchedulingActionDetailResponse(
        SchedulingActionSummaryResponse action,
        String targetAssetKey,
        String targetSnapshotId,
        Map<String, Object> requestPayload,
        BackfillActionEvidenceResponse backfillBatch,
        List<TaskSnapshotEvidenceResponse> snapshotEvidence) {
}
