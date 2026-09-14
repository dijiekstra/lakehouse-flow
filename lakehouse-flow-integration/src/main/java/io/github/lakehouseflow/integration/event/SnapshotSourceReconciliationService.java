package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.dao.SnapshotSourceHealthRepository;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSource;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSourceRegistry;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;
import io.github.lakehouseflow.integration.source.SnapshotSourcePosition;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EventConsumerOffset;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.model.SnapshotSourceHealth;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Reconciles live lakehouse metadata with durable offsets, events, and AssetState projections.
 *
 * <p>Automatic repair is deliberately bounded. It may ingest retained snapshots or replay an
 * already durable event projection, but it never advances an offset past missing source history
 * and never creates synthetic snapshot evidence.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SnapshotSourceReconciliationService {

    private final LakehouseSnapshotSourceRegistry sourceRegistry;
    private final EventConsumerOffsetRepository eventConsumerOffsetRepository;
    private final LakehouseEventRepository lakehouseEventRepository;
    private final AssetStateRepository assetStateRepository;
    private final EventIngestionService eventIngestionService;
    private final SnapshotIngestionTransactionService snapshotIngestionTransactionService;
    private final SnapshotSourceMetrics snapshotSourceMetrics;
    private final SnapshotSourceHealthRepository snapshotSourceHealthRepository;

    /**
     * Inspect every configured source without mutating ingestion state.
     *
     * @return immutable reconciliation results in configured source order
     */
    public List<SnapshotSourceReconciliation> reconcileAllSources() {
        return sourceRegistry.sources().stream()
                .map(this::inspectAndRecord)
                .toList();
    }

    /**
     * Inspect one configured source without mutating ingestion state.
     *
     * @param source configured snapshot source
     * @return reconciliation result
     */
    public SnapshotSourceReconciliation reconcileSource(LakehouseSnapshotSource source) {
        return inspectAndRecord(requireSource(source));
    }

    /**
     * Reconcile all sources and apply only bounded, evidence-preserving compensation.
     *
     * @return final reconciliation results after attempted repairs
     */
    public List<SnapshotSourceReconciliation> reconcileAndRepairAllSources() {
        return sourceRegistry.sources().stream()
                .map(this::repairAndRecord)
                .toList();
    }

    /** Inspect and publish metrics for one source. */
    private SnapshotSourceReconciliation inspectAndRecord(LakehouseSnapshotSource source) {
        SnapshotSourceReconciliation result = inspectSafely(source);
        persistHealth(result);
        snapshotSourceMetrics.recordReconciliation(result);
        return result;
    }

    /** Apply bounded scan or projection replay and publish only the final result. */
    private SnapshotSourceReconciliation repairAndRecord(LakehouseSnapshotSource source) {
        SnapshotSourceReconciliation initial = inspectSafely(source);
        SnapshotSourceReconciliation current = initial;
        boolean repairAttempted = false;
        int repairedEvents = 0;
        String repairError = null;

        try {
            if (current.outcome() == SnapshotSourceReconciliationOutcome.REPAIRABLE
                    && (current.offsetStatus() == SnapshotSourceOffsetStatus.UNINITIALIZED
                    || current.offsetStatus() == SnapshotSourceOffsetStatus.LAGGING)) {
                repairAttempted = true;
                repairedEvents += eventIngestionService.ingestSource(source);
                current = inspectSafely(source);
            }
            for (int pass = 0;
                    pass < 2
                            && current.outcome() == SnapshotSourceReconciliationOutcome.REPAIRABLE
                            && isProjectionRepairable(current.projectionStatus());
                    pass++) {
                repairAttempted = true;
                replayProjection(source, current);
                current = inspectSafely(source);
            }
        } catch (RuntimeException e) {
            repairError = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            log.error("Snapshot source reconciliation repair failed for {}/{}: {}",
                    source.identity().sourceType(), source.identity().sourceName(), repairError, e);
            current = inspectSafely(source);
        }

        String detail = repairError == null
                ? current.detail()
                : current.detail() + "; repair failed: " + repairError;
        SnapshotSourceReconciliation result = copyWithRepair(
                current,
                repairAttempted,
                repairedEvents,
                detail);
        persistHealth(result);
        snapshotSourceMetrics.recordReconciliation(result);
        return result;
    }

    /** Persist the final table-level reconciliation proof for cross-process confirmation. */
    private void persistHealth(SnapshotSourceReconciliation result) {
        LakehouseSourceIdentity identity = result.identity();
        SnapshotSourceHealth health = snapshotSourceHealthRepository
                .findBySourceTypeAndSourceName(identity.sourceType(), identity.sourceName())
                .orElseGet(() -> SnapshotSourceHealth.builder()
                        .sourceType(identity.sourceType())
                        .sourceName(identity.sourceName())
                        .tableAssetKey(identity.assetKey())
                        .createdAt(LocalDateTime.now())
                        .build());
        if (!identity.assetKey().equals(health.getTableAssetKey())) {
            throw new IllegalStateException(
                    "Snapshot source identity cannot be rebound from " + health.getTableAssetKey()
                            + " to " + identity.assetKey());
        }
        health.setOutcome(result.outcome().name());
        health.setOffsetStatus(result.offsetStatus().name());
        health.setProjectionStatus(result.projectionStatus().name());
        health.setDurableOffset(result.durableOffset());
        health.setLatestSourceOffset(result.latestSourceOffset());
        health.setEvidenceCheckedAt(result.checkedAt());
        health.setDetail(result.detail());
        snapshotSourceHealthRepository.save(health);
    }

    /** Inspect a source and convert adapter or repository failures into blocked evidence. */
    private SnapshotSourceReconciliation inspectSafely(LakehouseSnapshotSource source) {
        LakehouseSourceIdentity identity = source.identity();
        try {
            Optional<EventConsumerOffset> offset = eventConsumerOffsetRepository
                    .findBySourceTypeAndSourceName(identity.sourceType(), identity.sourceName());
            String durableOffset = offset.map(EventConsumerOffset::getOffsetValue).orElse(null);
            SnapshotSourcePosition position = source.inspectPosition(durableOffset);
            Optional<LakehouseEvent> latestEvent = latestEvent(identity);
            Optional<LakehouseEvent> latestDataEvent = latestDataEvent(identity);
            Optional<AssetState> assetState = assetStateRepository.findByAssetKey(identity.assetKey());
            SnapshotProjectionStatus projectionStatus = projectionStatus(
                    position,
                    latestEvent.orElse(null),
                    latestDataEvent.orElse(null),
                    assetState.orElse(null));
            SnapshotSourceReconciliationOutcome outcome = outcome(position.status(), projectionStatus);
            return new SnapshotSourceReconciliation(
                    identity,
                    outcome,
                    position.status(),
                    projectionStatus,
                    position.durableOffset(),
                    position.earliestRetainedOffset(),
                    position.latestSourceOffset(),
                    position.latestSnapshotId(),
                    latestEvent.map(LakehouseEvent::getSnapshotId).orElse(null),
                    assetState.map(AssetState::getLatestSnapshotId).orElse(null),
                    latestDataEvent.map(LakehouseEvent::getSnapshotId).orElse(null),
                    assetState.map(AssetState::getLatestDataSnapshotId).orElse(null),
                    position.pendingOffsetCount(),
                    false,
                    0,
                    detail(position.detail(), projectionStatus),
                    LocalDateTime.now());
        } catch (RuntimeException e) {
            String detail = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            return new SnapshotSourceReconciliation(
                    identity,
                    SnapshotSourceReconciliationOutcome.BLOCKED,
                    SnapshotSourceOffsetStatus.ERROR,
                    SnapshotProjectionStatus.ERROR,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    0,
                    detail,
                    LocalDateTime.now());
        }
    }

    /** Find the latest durable event for the configured source's unique physical asset. */
    private Optional<LakehouseEvent> latestEvent(LakehouseSourceIdentity identity) {
        return lakehouseEventRepository.findLatestSourceEvent(
                identity.sourceType(),
                identity.catalogName(),
                identity.databaseName(),
                identity.tableName());
    }

    /** Find the latest durable typed business-data event for the configured source. */
    private Optional<LakehouseEvent> latestDataEvent(LakehouseSourceIdentity identity) {
        return lakehouseEventRepository.findLatestDataSourceEvent(
                identity.sourceType(),
                identity.catalogName(),
                identity.databaseName(),
                identity.tableName());
    }

    /** Determine whether durable event and table-level AssetState evidence agree. */
    private SnapshotProjectionStatus projectionStatus(
            SnapshotSourcePosition position,
            LakehouseEvent latestEvent,
            LakehouseEvent latestDataEvent,
            AssetState assetState) {
        if (position.durableOffset() == null) {
            return latestEvent == null && assetState == null
                    ? SnapshotProjectionStatus.NOT_APPLICABLE
                    : SnapshotProjectionStatus.ORPHANED_PROJECTION;
        }
        if (latestEvent == null) {
            return SnapshotProjectionStatus.MISSING_EVENT;
        }
        if (assetState == null) {
            return SnapshotProjectionStatus.MISSING_ASSET_STATE;
        }
        if (!same(latestEvent.getSnapshotId(), assetState.getLatestSnapshotId())) {
            return SnapshotProjectionStatus.DRIFT;
        }
        if (position.status() == SnapshotSourceOffsetStatus.IN_SYNC
                && !same(position.latestSnapshotId(), latestEvent.getSnapshotId())) {
            return SnapshotProjectionStatus.EVENT_OFFSET_MISMATCH;
        }
        if (latestDataEvent == null) {
            return assetState.getLatestDataSnapshotId() == null
                    ? SnapshotProjectionStatus.CONSISTENT
                    : SnapshotProjectionStatus.ORPHANED_DATA_PROJECTION;
        }
        if (assetState.getLatestDataSnapshotId() == null) {
            return SnapshotProjectionStatus.MISSING_DATA_STATE;
        }
        if (!same(latestDataEvent.getSnapshotId(), assetState.getLatestDataSnapshotId())) {
            return SnapshotProjectionStatus.DATA_DRIFT;
        }
        return SnapshotProjectionStatus.CONSISTENT;
    }

    /** Convert offset and projection status into one operational outcome. */
    private SnapshotSourceReconciliationOutcome outcome(
            SnapshotSourceOffsetStatus offsetStatus,
            SnapshotProjectionStatus projectionStatus) {
        if (offsetStatus == SnapshotSourceOffsetStatus.ERROR
                || offsetStatus == SnapshotSourceOffsetStatus.RETENTION_GAP
                || offsetStatus == SnapshotSourceOffsetStatus.OFFSET_AHEAD
                || projectionStatus == SnapshotProjectionStatus.ERROR
                || projectionStatus == SnapshotProjectionStatus.MISSING_EVENT
                || projectionStatus == SnapshotProjectionStatus.ORPHANED_PROJECTION
                || projectionStatus == SnapshotProjectionStatus.ORPHANED_DATA_PROJECTION
                || projectionStatus == SnapshotProjectionStatus.EVENT_OFFSET_MISMATCH) {
            return SnapshotSourceReconciliationOutcome.BLOCKED;
        }
        if (offsetStatus == SnapshotSourceOffsetStatus.UNINITIALIZED
                || offsetStatus == SnapshotSourceOffsetStatus.LAGGING
                || projectionStatus == SnapshotProjectionStatus.MISSING_ASSET_STATE
                || projectionStatus == SnapshotProjectionStatus.DRIFT
                || projectionStatus == SnapshotProjectionStatus.MISSING_DATA_STATE
                || projectionStatus == SnapshotProjectionStatus.DATA_DRIFT) {
            return SnapshotSourceReconciliationOutcome.REPAIRABLE;
        }
        return SnapshotSourceReconciliationOutcome.HEALTHY;
    }

    /** Replay an existing durable event through the atomic projection path. */
    private void replayProjection(
            LakehouseSnapshotSource source,
            SnapshotSourceReconciliation reconciliation) {
        LakehouseSourceIdentity identity = source.identity();
        boolean dataProjection = reconciliation.projectionStatus() == SnapshotProjectionStatus.MISSING_DATA_STATE
                || reconciliation.projectionStatus() == SnapshotProjectionStatus.DATA_DRIFT;
        LakehouseEvent event = (dataProjection ? latestDataEvent(identity) : latestEvent(identity))
                .orElseThrow(() -> new IllegalStateException("Cannot replay a missing source projection event"));
        if (reconciliation.durableOffset() == null) {
            throw new IllegalStateException("Cannot replay projection without a durable source offset");
        }
        snapshotIngestionTransactionService.processSnapshot(
                identity.sourceName(),
                reconciliation.durableOffset(),
                source.offsetComparator(),
                event);
    }

    /** Determine whether one projection inconsistency can be repaired by durable event replay. */
    private boolean isProjectionRepairable(SnapshotProjectionStatus status) {
        return status == SnapshotProjectionStatus.MISSING_ASSET_STATE
                || status == SnapshotProjectionStatus.DRIFT
                || status == SnapshotProjectionStatus.MISSING_DATA_STATE
                || status == SnapshotProjectionStatus.DATA_DRIFT;
    }

    /** Copy a final inspection result with repair audit evidence. */
    private SnapshotSourceReconciliation copyWithRepair(
            SnapshotSourceReconciliation source,
            boolean repairAttempted,
            int repairedEvents,
            String detail) {
        return new SnapshotSourceReconciliation(
                source.identity(),
                source.outcome(),
                source.offsetStatus(),
                source.projectionStatus(),
                source.durableOffset(),
                source.earliestRetainedOffset(),
                source.latestSourceOffset(),
                source.latestSourceSnapshotId(),
                source.latestEventSnapshotId(),
                source.assetStateSnapshotId(),
                source.latestDataEventSnapshotId(),
                source.assetStateDataSnapshotId(),
                source.pendingOffsetCount(),
                repairAttempted,
                repairedEvents,
                detail,
                LocalDateTime.now());
    }

    /** Validate one caller-supplied source. */
    private LakehouseSnapshotSource requireSource(LakehouseSnapshotSource source) {
        if (source == null) {
            throw new IllegalArgumentException("snapshot source must not be null");
        }
        return source;
    }

    /** Compare nullable snapshot coordinates without format-specific interpretation. */
    private boolean same(String left, String right) {
        return left == null ? right == null : left.equals(right);
    }

    /** Combine adapter position evidence with projection status. */
    private String detail(String positionDetail, SnapshotProjectionStatus projectionStatus) {
        String prefix = positionDetail == null || positionDetail.isBlank()
                ? "Source position inspected"
                : positionDetail;
        return prefix + "; projection=" + projectionStatus;
    }
}
