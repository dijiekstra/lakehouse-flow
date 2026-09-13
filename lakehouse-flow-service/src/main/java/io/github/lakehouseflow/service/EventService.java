package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.model.LakehouseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Service for ingesting and deduplicating lakehouse events.
 *
 * Key responsibility:
 * - Detect and ignore duplicate events (same event_id)
 * - Persist events to durable storage
 * - Trigger asset state updates
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class EventService {

    private final LakehouseEventRepository eventRepository;
    private final AssetStateService assetStateService;

    /**
     * Ingest an event:
     * 1. Check if already exists (deduplication)
     * 2. If new, persist it
     * 3. Update asset state from event
     *
     * Returns: the LakehouseEvent (either new or existing)
     */
    public LakehouseEvent ingestEvent(LakehouseEvent event) {
        // Set observed time if not provided
        if (event.getObservedAt() == null) {
            event.setObservedAt(LocalDateTime.now());
        }

        Optional<LakehouseEvent> existing = eventRepository.findByEventId(event.getEventId());
        if (existing.isPresent()) {
            log.debug("Event {} already exists, projecting existing event into asset state", event.getEventId());
            assetStateService.projectAssetStatesFromEvent(existing.get());
            return existing.get();
        }

        // Try to persist
        try {
            log.info("Ingesting event {} from {}", event.getEventId(), event.getSourceType());
            LakehouseEvent saved = eventRepository.save(event);

            // Update asset state based on this new event
            assetStateService.projectAssetStatesFromEvent(saved);

            return saved;
        } catch (DataIntegrityViolationException e) {
            // Event already exists, this is normal (deduplication)
            log.debug("Event {} already exists, skipping", event.getEventId());
            Optional<LakehouseEvent> concurrentExisting = eventRepository.findByEventId(event.getEventId());
            LakehouseEvent saved = concurrentExisting
                    .orElseThrow(() -> new RuntimeException("Event should exist but not found"));
            assetStateService.projectAssetStatesFromEvent(saved);
            return saved;
        }
    }

    /**
     * Find event by ID
     */
    public Optional<LakehouseEvent> findEventById(String eventId) {
        return eventRepository.findByEventId(eventId);
    }

    /**
     * Get latest event for an asset
     */
    public Optional<LakehouseEvent> getLatestEventForAsset(
            String catalogName,
            String databaseName,
            String tableName,
            String partitionName) {
        return eventRepository.findLatestEventByAsset(catalogName, databaseName, tableName, partitionName);
    }
}
