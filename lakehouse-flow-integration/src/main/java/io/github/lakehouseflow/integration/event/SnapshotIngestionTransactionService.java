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
        advanceOffset(
                candidateEvent.getSourceType(),
                sourceName,
                sourceOffset,
                offsetComparator);
        return new SnapshotIngestionResult(inserted, event.getEventId(), sourceOffset);
    }

    /**
     * Advance a source offset without allowing an older snapshot to overwrite it.
     *
     * @param sourceType lake format or source type
     * @param sourceName stable source name
     * @param sourceOffset candidate snapshot offset
     * @param offsetComparator format-specific offset ordering
     */
    private void advanceOffset(
            String sourceType,
            String sourceName,
            String sourceOffset,
            Comparator<String> offsetComparator) {
        if (offsetComparator == null) {
            throw new IllegalArgumentException("offsetComparator must not be null");
        }
        Optional<EventConsumerOffset> existing =
                eventConsumerOffsetRepository.findForUpdate(sourceType, sourceName);
        if (existing.isPresent()) {
            EventConsumerOffset offset = existing.get();
            if (offsetComparator.compare(sourceOffset, offset.getOffsetValue()) > 0) {
                offset.setOffsetValue(sourceOffset);
                offset.setUpdatedAt(LocalDateTime.now());
                eventConsumerOffsetRepository.save(offset);
            }
            return;
        }
        eventConsumerOffsetRepository.save(EventConsumerOffset.builder()
                .sourceType(sourceType)
                .sourceName(sourceName)
                .offsetValue(sourceOffset)
                .build());
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
