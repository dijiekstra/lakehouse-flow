package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.model.AssetState;
import io.github.lakehouseflow.model.LakehouseEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests AssetStateService public methods against repository interactions.
 */
@ExtendWith(MockitoExtension.class)
class AssetStateServiceTest {

    @Mock
    private AssetStateRepository assetStateRepository;

    @InjectMocks
    private AssetStateService assetStateService;

    /**
     * Verify a first event creates the asset state used as scheduling truth.
     */
    @Test
    void updateAssetStateFromEventCreatesStateWhenMissing() {
        LakehouseEvent event = event("100", LocalDateTime.of(2026, 9, 12, 1, 0), null);
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.empty());
        when(assetStateRepository.save(any(AssetState.class))).thenAnswer(invocation -> invocation.getArgument(0));

        AssetState result = assetStateService.updateAssetStateFromEvent(event);

        assertEquals("paimon.prod.orders", result.getAssetKey());
        assertEquals("100", result.getLatestSnapshotId());
        assertEquals("100", result.getLatestDataSnapshotId());
        assertEquals("UNKNOWN", result.getReadinessStatus());
        verify(assetStateRepository).save(any(AssetState.class));
    }

    /**
     * Verify newer snapshot evidence updates the existing AssetState.
     */
    @Test
    void updateAssetStateFromEventUpdatesExistingStateWhenSnapshotAdvances() {
        AssetState state = assetState("100", LocalDateTime.of(2026, 9, 12, 1, 0));
        LakehouseEvent event = event("101", LocalDateTime.of(2026, 9, 12, 2, 0), null);
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));
        when(assetStateRepository.save(state)).thenReturn(state);

        AssetState result = assetStateService.updateAssetStateFromEvent(event);

        assertSame(state, result);
        assertEquals("101", state.getLatestSnapshotId());
        assertEquals("101", state.getLatestDataSnapshotId());
        assertEquals(LocalDateTime.of(2026, 9, 12, 2, 0), state.getLatestWatermark());
        assertEquals(LocalDateTime.of(2026, 9, 12, 2, 0), state.getLatestDataWatermark());
        verify(assetStateRepository).save(state);
    }

    /**
     * Verify source-native schema ids follow the newer snapshot without lexical comparison.
     */
    @Test
    void updateAssetStateFromEventAssociatesSchemaWithNewerSnapshot() {
        AssetState state = assetState("100", LocalDateTime.of(2026, 9, 12, 1, 0));
        state.setLatestSchemaId("schema-z");
        LakehouseEvent event = event("101", LocalDateTime.of(2026, 9, 12, 2, 0), null);
        event.setSchemaId("schema-a");
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));
        when(assetStateRepository.save(state)).thenReturn(state);

        assetStateService.updateAssetStateFromEvent(event);

        assertEquals("101", state.getLatestSnapshotId());
        assertEquals("schema-a", state.getLatestSchemaId());
    }

    /**
     * Verify stale snapshot evidence does not move AssetState backward.
     */
    @Test
    void updateAssetStateFromEventDoesNotRegressSnapshotOrWatermark() {
        AssetState state = assetState("100", LocalDateTime.of(2026, 9, 12, 1, 0));
        LakehouseEvent event = event("99", LocalDateTime.of(2026, 9, 12, 0, 30), null);
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));

        AssetState result = assetStateService.updateAssetStateFromEvent(event);

        assertSame(state, result);
        assertEquals("100", state.getLatestSnapshotId());
        assertEquals(LocalDateTime.of(2026, 9, 12, 1, 0), state.getLatestWatermark());
    }

    /**
     * Verify a newer snapshot cannot overwrite a newer watermark with stale event time.
     */
    @Test
    void updateAssetStateFromEventAdvancesSnapshotWithoutRegressingWatermark() {
        AssetState state = assetState("100", LocalDateTime.of(2026, 9, 12, 2, 0));
        LakehouseEvent event = event("101", LocalDateTime.of(2026, 9, 12, 1, 0), null);
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));
        when(assetStateRepository.save(state)).thenReturn(state);

        AssetState result = assetStateService.updateAssetStateFromEvent(event);

        assertSame(state, result);
        assertEquals("101", state.getLatestSnapshotId());
        assertEquals(LocalDateTime.of(2026, 9, 12, 2, 0), state.getLatestWatermark());
    }

    /**
     * Verify partitioned events build partition-aware asset keys.
     */
    @Test
    void updateAssetStateFromEventUsesPartitionedAssetKey() {
        LakehouseEvent event = event("100", LocalDateTime.of(2026, 9, 12, 1, 0), "dt=2026-09-12");
        when(assetStateRepository.findByAssetKey("paimon.prod.orders.dt=2026-09-12")).thenReturn(Optional.empty());
        when(assetStateRepository.save(any(AssetState.class))).thenAnswer(invocation -> invocation.getArgument(0));

        AssetState result = assetStateService.updateAssetStateFromEvent(event);

        assertEquals("paimon.prod.orders.dt=2026-09-12", result.getAssetKey());
    }

    /**
     * Verify one table snapshot projects independent state for every changed partition.
     */
    @Test
    void projectAssetStatesFromEventCreatesTableAndChangedPartitionStates() {
        LakehouseEvent event = event("100", LocalDateTime.of(2026, 9, 12, 1, 0), "dt=2026-09-11");
        event.setPayloadJson(Map.of(
                "changedPartitions",
                List.of("dt=2026-09-11", "dt=2026-09-12")));
        when(assetStateRepository.findByAssetKey(anyString())).thenReturn(Optional.empty());
        when(assetStateRepository.save(any(AssetState.class))).thenAnswer(invocation -> invocation.getArgument(0));

        List<AssetState> states = assetStateService.projectAssetStatesFromEvent(event);

        assertEquals(List.of(
                        "paimon.prod.orders",
                        "paimon.prod.orders.dt=2026-09-11",
                        "paimon.prod.orders.dt=2026-09-12"),
                states.stream().map(AssetState::getAssetKey).toList());
        assertEquals(3, states.size());
        assertEquals("PARTITION", states.get(1).getAssetType());
    }

    /** Verify compaction advances only physical table state and never creates partition progress. */
    @Test
    void projectAssetStatesFromEventKeepsCompactionOffDataTrack() {
        AssetState state = assetState("100", LocalDateTime.of(2026, 9, 12, 1, 0));
        LakehouseEvent event = event("101", LocalDateTime.of(2026, 9, 12, 2, 0), null);
        event.setCommitKind("COMPACT");
        event.setDataChange(false);
        event.setPayloadJson(Map.of("changedPartitions", List.of("dt=2026-09-12")));
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));
        when(assetStateRepository.save(state)).thenReturn(state);

        List<AssetState> states = assetStateService.projectAssetStatesFromEvent(event);

        assertEquals(1, states.size());
        assertEquals("101", state.getLatestSnapshotId());
        assertEquals("100", state.getLatestDataSnapshotId());
        assertEquals(LocalDateTime.of(2026, 9, 12, 1, 0), state.getLatestDataWatermark());
        verify(assetStateRepository, never())
                .findByAssetKey("paimon.prod.orders.dt=2026-09-12");
    }

    /** Verify an older durable data event can repair data state after a newer compaction. */
    @Test
    void updateAssetStateFromEventRepairsDataTrackWithoutRegressingObservedTrack() {
        AssetState state = assetState("102", LocalDateTime.of(2026, 9, 12, 3, 0));
        state.setLatestDataSnapshotId(null);
        state.setLatestDataWatermark(null);
        state.setLatestDataCommitTime(null);
        LakehouseEvent event = event("101", LocalDateTime.of(2026, 9, 12, 2, 0), null);
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));
        when(assetStateRepository.save(state)).thenReturn(state);

        AssetState result = assetStateService.updateAssetStateFromEvent(event);

        assertSame(state, result);
        assertEquals("102", state.getLatestSnapshotId());
        assertEquals("101", state.getLatestDataSnapshotId());
        assertEquals(LocalDateTime.of(2026, 9, 12, 2, 0), state.getLatestDataWatermark());
        verify(assetStateRepository).save(state);
    }

    /**
     * Verify asset-state lookup delegates to the repository.
     */
    @Test
    void getAssetStateReturnsRepositoryResult() {
        AssetState state = assetState("100", LocalDateTime.of(2026, 9, 12, 1, 0));
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));

        Optional<AssetState> result = assetStateService.getAssetState("paimon.prod.orders");

        assertTrue(result.isPresent());
        assertSame(state, result.get());
    }

    /**
     * Verify marking an existing asset ready updates readiness state.
     */
    @Test
    void markAsReadyUpdatesExistingAsset() {
        AssetState state = assetState("100", LocalDateTime.of(2026, 9, 12, 1, 0));
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.of(state));
        when(assetStateRepository.save(state)).thenReturn(state);

        AssetState result = assetStateService.markAsReady("paimon.prod.orders");

        assertSame(state, result);
        assertEquals("READY", state.getReadinessStatus());
        verify(assetStateRepository).save(state);
    }

    /**
     * Verify marking a missing asset ready keeps the old null-return contract.
     */
    @Test
    void markAsReadyReturnsNullWhenAssetMissing() {
        when(assetStateRepository.findByAssetKey("paimon.prod.orders")).thenReturn(Optional.empty());

        AssetState result = assetStateService.markAsReady("paimon.prod.orders");

        assertNull(result);
        verify(assetStateRepository, never()).save(any(AssetState.class));
    }

    /**
     * Build an AssetState fixture.
     */
    private AssetState assetState(String snapshotId, LocalDateTime watermark) {
        return AssetState.builder()
                .assetKey("paimon.prod.orders")
                .assetType("TABLE")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .latestSnapshotId(snapshotId)
                .latestDataSnapshotId(snapshotId)
                .latestWatermark(watermark)
                .latestDataWatermark(watermark)
                .latestCommitTime(watermark)
                .latestDataCommitTime(watermark)
                .readinessStatus("UNKNOWN")
                .build();
    }

    /**
     * Build a lakehouse event fixture.
     */
    private LakehouseEvent event(String snapshotId, LocalDateTime watermark, String partitionName) {
        return LakehouseEvent.builder()
                .eventId("event-" + snapshotId)
                .sourceType("PAIMON")
                .eventType("SNAPSHOT_COMMITTED")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .partitionName(partitionName)
                .snapshotId(snapshotId)
                .schemaId("schema-1")
                .commitKind("APPEND")
                .dataChange(true)
                .watermark(watermark)
                .commitTime(watermark)
                .build();
    }
}
