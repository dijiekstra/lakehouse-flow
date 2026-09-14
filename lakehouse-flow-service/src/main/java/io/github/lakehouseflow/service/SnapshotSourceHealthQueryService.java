package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.AssetKeys;
import io.github.lakehouseflow.common.SnapshotSourceHealthOutcomes;
import io.github.lakehouseflow.dao.SnapshotSourceHealthRepository;
import io.github.lakehouseflow.model.SnapshotSourceHealth;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Read-only operations queries over durable snapshot-source reconciliation evidence.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SnapshotSourceHealthQueryService {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 500;
    private static final Set<String> EXTERNAL_SOURCE_HEALTH = Set.of(
            SnapshotSourceHealthOutcomes.HEALTHY,
            SnapshotSourceHealthOutcomes.REPAIRABLE,
            SnapshotSourceHealthOutcomes.SOURCE_BLOCKED);

    private final SnapshotSourceHealthRepository snapshotSourceHealthRepository;

    /**
     * Find latest persisted source reconciliation evidence using optional operational filters.
     *
     * <p>Persisted BLOCKED evidence is exposed as SOURCE_BLOCKED so callers see the same result
     * vocabulary used by task and job-control snapshot confirmation.
     *
     * @param sourceType optional lake-format source type
     * @param sourceName optional configured source name
     * @param flowCode optional Flow using the managed output table
     * @param targetAssetKey optional table or partition target
     * @param sourceHealth optional HEALTHY, REPAIRABLE, or SOURCE_BLOCKED result
     * @param requestedLimit maximum rows, or null for the default
     * @return latest matching reconciliation evidence first
     */
    public List<SnapshotSourceEvidence> findLatestEvidence(
            String sourceType,
            String sourceName,
            String flowCode,
            String targetAssetKey,
            String sourceHealth,
            Integer requestedLimit) {
        String normalizedSourceHealth = normalizeSourceHealth(sourceHealth);
        String persistedOutcome = SnapshotSourceHealthOutcomes.SOURCE_BLOCKED.equals(normalizedSourceHealth)
                ? SnapshotSourceHealthOutcomes.BLOCKED
                : normalizedSourceHealth;
        return snapshotSourceHealthRepository.findLatestEvidence(
                        normalizeUppercase(sourceType),
                        normalizeText(sourceName),
                        normalizeText(flowCode),
                        normalizeTableAssetKey(targetAssetKey),
                        persistedOutcome,
                        PageRequest.of(0, validateLimit(requestedLimit)))
                .stream()
                .map(this::toEvidence)
                .toList();
    }

    /** Convert one persisted reconciliation record into the external source result vocabulary. */
    private SnapshotSourceEvidence toEvidence(SnapshotSourceHealth health) {
        String sourceHealth = SnapshotSourceHealthOutcomes.BLOCKED.equals(health.getOutcome())
                ? SnapshotSourceHealthOutcomes.SOURCE_BLOCKED
                : health.getOutcome();
        return new SnapshotSourceEvidence(
                health.getId(),
                health.getSourceType(),
                health.getSourceName(),
                health.getTableAssetKey(),
                sourceHealth,
                health.getOffsetStatus(),
                health.getProjectionStatus(),
                health.getDurableOffset(),
                health.getLatestSourceOffset(),
                health.getEvidenceCheckedAt(),
                health.getDetail(),
                health.getUpdatedAt());
    }

    /** Normalize an optional source-health filter into the stable external vocabulary. */
    private String normalizeSourceHealth(String sourceHealth) {
        String normalized = normalizeUppercase(sourceHealth);
        if (normalized != null && !EXTERNAL_SOURCE_HEALTH.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported source health: " + sourceHealth);
        }
        return normalized;
    }

    /** Normalize a table or partition key to the managed physical table identity. */
    private String normalizeTableAssetKey(String targetAssetKey) {
        if (targetAssetKey == null || targetAssetKey.isBlank()) {
            return null;
        }
        return AssetKeys.tableKey(targetAssetKey.trim())
                .orElseThrow(() -> new IllegalArgumentException(
                        "targetAssetKey must be catalog.database.table[.partition]"));
    }

    /** Normalize optional free text by trimming it. */
    private String normalizeText(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    /** Normalize an optional case-insensitive enum-like filter. */
    private String normalizeUppercase(String value) {
        String normalized = normalizeText(value);
        return normalized == null ? null : normalized.toUpperCase(Locale.ROOT);
    }

    /** Validate a bounded operations query size. */
    private int validateLimit(Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_LIMIT : requestedLimit;
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("snapshot-source query limit must be between 1 and " + MAX_LIMIT);
        }
        return limit;
    }

    /**
     * Persisted reconciliation evidence for one managed physical table.
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
    public record SnapshotSourceEvidence(
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
}
