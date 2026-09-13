package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests EventService public methods for event deduplication and lookups.
 */
@ExtendWith(MockitoExtension.class)
class EventServiceTest {

    @Mock
    private LakehouseEventRepository eventRepository;

    @Mock
    private AssetStateService assetStateService;

    @InjectMocks
    private EventService eventService;

    /**
     * Verify a new event is saved and projected into AssetState.
     */
    @Test
    void ingestEventPersistsNewEventAndUpdatesAssetState() {
        LakehouseEvent event = event("event-100");
        when(eventRepository.findByEventId("event-100")).thenReturn(Optional.empty());
        when(eventRepository.save(event)).thenReturn(event);

        LakehouseEvent result = eventService.ingestEvent(event);

        assertSame(event, result);
        assertNotNull(event.getObservedAt());
        verify(assetStateService).projectAssetStatesFromEvent(event);
    }

    /**
     * Verify duplicate event ingestion returns the existing event.
     */
    @Test
    void ingestEventReturnsExistingEventWhenAlreadyStored() {
        LakehouseEvent incoming = event("event-100");
        LakehouseEvent existing = event("event-100");
        when(eventRepository.findByEventId("event-100")).thenReturn(Optional.of(existing));

        LakehouseEvent result = eventService.ingestEvent(incoming);

        assertSame(existing, result);
        verify(assetStateService).projectAssetStatesFromEvent(existing);
    }

    /**
     * Verify concurrent unique-key conflicts are handled as deduplication.
     */
    @Test
    void ingestEventHandlesConcurrentDuplicateInsert() {
        LakehouseEvent event = event("event-100");
        when(eventRepository.findByEventId("event-100"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(event));
        when(eventRepository.save(event)).thenThrow(new DataIntegrityViolationException("duplicate"));

        LakehouseEvent result = eventService.ingestEvent(event);

        assertSame(event, result);
        verify(assetStateService).projectAssetStatesFromEvent(event);
    }

    /**
     * Verify event lookup by event id delegates to the repository.
     */
    @Test
    void findEventByIdReturnsRepositoryResult() {
        LakehouseEvent event = event("event-100");
        when(eventRepository.findByEventId("event-100")).thenReturn(Optional.of(event));

        Optional<LakehouseEvent> result = eventService.findEventById("event-100");

        assertTrue(result.isPresent());
        assertSame(event, result.get());
    }

    /**
     * Verify latest event lookup for an asset delegates to the repository.
     */
    @Test
    void getLatestEventForAssetReturnsRepositoryResult() {
        LakehouseEvent event = event("event-100");
        when(eventRepository.findLatestEventByAsset("paimon", "prod", "orders", null))
                .thenReturn(Optional.of(event));

        Optional<LakehouseEvent> result = eventService.getLatestEventForAsset("paimon", "prod", "orders", null);

        assertTrue(result.isPresent());
        assertSame(event, result.get());
    }

    /**
     * Build a lakehouse event fixture.
     */
    private LakehouseEvent event(String eventId) {
        return LakehouseEvent.builder()
                .eventId(eventId)
                .eventType("SNAPSHOT_COMMITTED")
                .sourceType("PAIMON")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .snapshotId("100")
                .commitTime(LocalDateTime.of(2026, 9, 12, 1, 0))
                .build();
    }
}
