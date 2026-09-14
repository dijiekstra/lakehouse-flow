package io.github.lakehouseflow.integration.event;

import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.EventConsumerOffset;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.service.AssetStateService;
import io.github.lakehouseflow.service.DagProgressionService;
import io.github.lakehouseflow.service.FlowPlanEvaluationService;
import io.github.lakehouseflow.service.SnapshotTriggerRoutingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Commits one source snapshot through the complete scheduling projection path.
 *
 * Event persistence, table/partition AssetState projection, trigger routing,
 * DAG evaluation, and source offset advancement share one transaction so a
 * failed scheduling decision cannot be skipped by a separately committed offset.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SnapshotIngestionTransactionService {

    private final LakehouseEventRepository lakehouseEventRepository;
    private final EventConsumerOffsetRepository eventConsumerOffsetRepository;
    private final AssetStateService assetStateService;
    private final DagProgressionService dagProgressionService;
    private final FlowPlanEvaluationService flowPlanEvaluationService;
    private final SnapshotTriggerRoutingService snapshotTriggerRoutingService;

    /**
     * Process one mapped lakehouse event and advance its source offset atomically.
     *
     * @param sourceName stable catalog/database/table source name
     * @param sourceOffset opaque source position used as consumer offset
     * @param offsetComparator format-specific offset ordering
     * @param candidateEvent event mapped from the source snapshot
     * @return whether the event was newly inserted or already present
     */
    @Transactional
    public SnapshotIngestionResult processSnapshot(
            String sourceName,
            String sourceOffset,
            Comparator<String> offsetComparator,
            LakehouseEvent candidateEvent) {

        EventConsumerOffset lockedOffset = lockSourceOffset(
                candidateEvent,
                sourceName,
                sourceOffset,
                offsetComparator);
        Optional<LakehouseEvent> existing = lakehouseEventRepository.findByEventId(candidateEvent.getEventId());
        boolean inserted = existing.isEmpty();
        LakehouseEvent event = existing.orElseGet(() -> lakehouseEventRepository.save(candidateEvent));
        List<AssetState> projectedStates = assetStateService.projectAssetStatesFromEvent(event);
        List<AssetState> advancedStates = projectedStates.stream()
                .filter(state -> event.getSnapshotId() != null
                        && event.getSnapshotId().equals(state.getLatestSnapshotId()))
                .toList();
        if (!advancedStates.isEmpty()) {
            SnapshotTriggerRoutingService.SnapshotTriggerRoute route =
                    snapshotTriggerRoutingService.classify(event);
            if (route.naturalProgressionAllowed()) {
                LocalDateTime bizDate = route.intentBizDate() != null
                        ? route.intentBizDate()
                        : event.getWatermark() != null ? event.getWatermark() : event.getCommitTime();
                advancedStates.forEach(state ->
                        dagProgressionService.onAssetStateAdvanced(state.getAssetKey(), bizDate));
                flowPlanEvaluationService.evaluateTriggeredAssets(
                        advancedStates.stream().map(AssetState::getAssetKey).toList(),
                        event.getSnapshotId(),
                        bizDate);
            } else {
                log.debug("Suppressed natural progression for snapshot event {}: origin={}, reason={}",
                        event.getEventId(), route.originType(), route.reason());
            }
        }
        advanceOffset(lockedOffset, sourceOffset, offsetComparator);
        return new SnapshotIngestionResult(inserted, event.getEventId(), sourceOffset);
    }

    /**
     * Initialize and lock one source before any event projection is performed.
     *
     * <p>The row lock is deliberately held across event persistence, AssetState projection,
     * trigger evaluation, and offset advancement. This makes concurrent scheduler nodes process
     * one source serially without separating the durable offset from its projections.
     *
     * @param candidateEvent event whose source owns the offset
     * @param sourceName stable source name
     * @param sourceOffset first or next candidate source position
     * @param offsetComparator format-specific offset ordering
     * @return locked durable source offset
     */
    private EventConsumerOffset lockSourceOffset(
            LakehouseEvent candidateEvent,
            String sourceName,
            String sourceOffset,
            Comparator<String> offsetComparator) {
        if (candidateEvent == null) {
            throw new IllegalArgumentException("candidateEvent must not be null");
        }
        if (offsetComparator == null) {
            throw new IllegalArgumentException("offsetComparator must not be null");
        }
        String sourceType = requireText(candidateEvent.getSourceType(), "candidateEvent.sourceType");
        String normalizedSourceName = requireText(sourceName, "sourceName");
        String normalizedSourceOffset = requireText(sourceOffset, "sourceOffset");
        eventConsumerOffsetRepository.ensureOffset(
                sourceType,
                normalizedSourceName,
                normalizedSourceOffset);
        return eventConsumerOffsetRepository.findForUpdate(sourceType, normalizedSourceName)
                .orElseThrow(() -> new IllegalStateException(
                        "Source offset initialization failed: " + sourceType + ":" + normalizedSourceName));
    }

    /**
     * Advance a locked source offset without allowing an older snapshot to overwrite it.
     *
     * @param offset locked durable source position
     * @param sourceOffset candidate source position
     * @param offsetComparator format-specific offset ordering
     */
    private void advanceOffset(
            EventConsumerOffset offset,
            String sourceOffset,
            Comparator<String> offsetComparator) {
        if (offsetComparator.compare(sourceOffset, offset.getOffsetValue()) > 0) {
            offset.setOffsetValue(sourceOffset);
            offset.setUpdatedAt(LocalDateTime.now());
            eventConsumerOffsetRepository.save(offset);
        }
    }

    /** Require and normalize one source coordination value. */
    private String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /**
     * Result of one atomic source snapshot projection.
     *
     * @param inserted whether a new event row was inserted
     * @param eventId processed event id
     * @param sourceOffset committed source offset
     */
    public record SnapshotIngestionResult(
            boolean inserted,
            String eventId,
            String sourceOffset) {
    }
}
