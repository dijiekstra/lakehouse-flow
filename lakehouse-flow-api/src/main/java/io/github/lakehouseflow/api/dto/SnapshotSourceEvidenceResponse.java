package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;

/**
 * API response for durable source-to-snapshot projection reconciliation evidence.
 *
 * @param id source-health row id
 * @param sourceType lake-format source type
 * @param sourceName configured source name
 * @param tableAssetKey managed physical table
 * @param sourceHealth external HEALTHY, REPAIRABLE, or SOURCE_BLOCKED result
 * @param offsetStatus relationship between durable and live source offsets
 * @param projectionStatus relationship between durable events and AssetState
 * @param durableOffset last completely projected source offset
 * @param latestSourceOffset latest source offset seen during reconciliation
 * @param evidenceCheckedAt actual source inspection time
 * @param detail human-readable reconciliation evidence
 * @param updatedAt persisted record update time
 */
public record SnapshotSourceEvidenceResponse(
        Long id,
        String sourceType,
        String sourceName,
        String tableAssetKey,
        String sourceHealth,
        String offsetStatus,
        String projectionStatus,
        String durableOffset,
        String latestSourceOffset,
        LocalDateTime evidenceCheckedAt,
        String detail,
        LocalDateTime updatedAt) {
}
