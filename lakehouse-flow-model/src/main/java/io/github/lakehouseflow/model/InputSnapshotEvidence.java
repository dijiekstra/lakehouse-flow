package io.github.lakehouseflow.model;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable input boundary frozen into one scheduling intent.
 *
 * @param parentNodeCode direct parent node, or null for an external asset condition
 * @param parentProcessingMode STREAMING or BATCH for a DAG parent, or null
 * @param evidenceSource source of the scheduling evidence
 * @param assetKey date-resolved input asset key
 * @param snapshotId business-data snapshot coordinate, or null
 * @param watermark business-data watermark, or null
 * @param upstreamTaskInstanceId same-instance parent task for action or batch evidence, or null
 * @param observedAt time represented by the frozen evidence
 */
public record InputSnapshotEvidence(
        String parentNodeCode,
        String parentProcessingMode,
        String evidenceSource,
        String assetKey,
        String snapshotId,
        String watermark,
        Long upstreamTaskInstanceId,
        LocalDateTime observedAt) {

    /**
     * Convert the evidence to a JSON-compatible ordered map.
     *
     * @return immutable contract payload fragment
     */
    public Map<String, Object> toPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("parentNodeCode", parentNodeCode);
        payload.put("parentProcessingMode", parentProcessingMode);
        payload.put("evidenceSource", evidenceSource);
        payload.put("assetKey", assetKey);
        payload.put("snapshotId", snapshotId);
        payload.put("watermark", watermark);
        payload.put("upstreamTaskInstanceId", upstreamTaskInstanceId);
        payload.put("observedAt", observedAt == null ? null : observedAt.toString());
        return Collections.unmodifiableMap(payload);
    }
}
