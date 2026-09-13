package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.model.SchedulingIntent;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests snapshot origin routing between natural and action-owned progression.
 */
@ExtendWith(MockitoExtension.class)
class SnapshotTriggerRoutingServiceTest {

    private static final LocalDateTime BIZ_DATE = LocalDateTime.of(2026, 9, 1, 0, 0);
    private static final String INTENT_KEY = "task-instance:42";
    private static final String TARGET_ASSET = "paimon.prod.orders.dt=2026-09-01";

    @Mock
    private SchedulingIntentRepository schedulingIntentRepository;

    @InjectMocks
    private SnapshotTriggerRoutingService routingService;

    /**
     * Verify an external data commit remains eligible for natural scheduling.
     */
    @Test
    void classifyAllowsExternalDataSnapshot() {
        LakehouseEvent event = event("APPEND", Map.of(), List.of("dt=2026-09-01"));

        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(event);

        assertTrue(route.naturalProgressionAllowed());
        assertEquals("EXTERNAL", route.originType());
        assertNull(route.intentKey());
        verifyNoInteractions(schedulingIntentRepository);
    }

    /**
     * Verify a final backfill snapshot only progresses its owning instance.
     */
    @Test
    void classifySuppressesNaturalTriggerForBackfillSnapshot() {
        SchedulingIntent intent = intent("BACKFILL");
        when(schedulingIntentRepository.findByIntentKey(INTENT_KEY)).thenReturn(Optional.of(intent));

        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(
                event("OVERWRITE", requiredProperties("true"), List.of("dt=2026-09-01")));

        assertFalse(route.naturalProgressionAllowed());
        assertEquals("LAKEHOUSE_FLOW_ACTION", route.originType());
        assertEquals(INTENT_KEY, route.intentKey());
        assertEquals(BIZ_DATE, route.intentBizDate());
    }

    /**
     * Verify a valid final snapshot from a natural intent may trigger downstream plans.
     */
    @Test
    void classifyAllowsFinalNaturalIntentSnapshot() {
        SchedulingIntent intent = intent("SNAPSHOT_DRIVEN");
        when(schedulingIntentRepository.findByIntentKey(INTENT_KEY)).thenReturn(Optional.of(intent));

        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(
                event("APPEND", requiredProperties("true"), List.of("dt=2026-09-01")));

        assertTrue(route.naturalProgressionAllowed());
        assertEquals("LAKEHOUSE_FLOW_NATURAL", route.originType());
        assertEquals(BIZ_DATE, route.intentBizDate());
    }

    /**
     * Verify an intermediate commit from a valid intent cannot trigger another flow.
     */
    @Test
    void classifySuppressesIntermediateIntentSnapshot() {
        when(schedulingIntentRepository.findByIntentKey(INTENT_KEY))
                .thenReturn(Optional.of(intent("SNAPSHOT_DRIVEN")));

        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(
                event("APPEND", requiredProperties("false"), List.of("dt=2026-09-01")));

        assertFalse(route.naturalProgressionAllowed());
        assertEquals("LAKEHOUSE_FLOW_INTERMEDIATE", route.originType());
    }

    /**
     * Verify copied properties cannot attribute a snapshot from another table.
     */
    @Test
    void classifySuppressesSnapshotFromWrongPhysicalTable() {
        when(schedulingIntentRepository.findByIntentKey(INTENT_KEY))
                .thenReturn(Optional.of(intent("SNAPSHOT_DRIVEN")));
        LakehouseEvent event = event("APPEND", requiredProperties("true"), List.of("dt=2026-09-01"));
        event.setTableName("payments");

        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(event);

        assertFalse(route.naturalProgressionAllowed());
        assertEquals("LAKEHOUSE_FLOW_INTERMEDIATE", route.originType());
    }

    /**
     * Verify a claimed Lakehouse Flow key that is not persisted fails closed.
     */
    @Test
    void classifySuppressesOrphanIntentMarker() {
        when(schedulingIntentRepository.findByIntentKey(INTENT_KEY)).thenReturn(Optional.empty());

        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(
                event("APPEND", requiredProperties("true"), List.of("dt=2026-09-01")));

        assertFalse(route.naturalProgressionAllowed());
        assertEquals("LAKEHOUSE_FLOW_ORPHAN", route.originType());
        verify(schedulingIntentRepository).findByIntentKey(INTENT_KEY);
    }

    /**
     * Verify maintenance snapshots update facts without entering natural triggering.
     */
    @Test
    void classifySuppressesExternalMaintenanceSnapshot() {
        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(
                event("COMPACT", Map.of(), List.of("dt=2026-09-01")));

        assertFalse(route.naturalProgressionAllowed());
        assertEquals("MAINTENANCE", route.originType());
        verifyNoInteractions(schedulingIntentRepository);
    }

    /**
     * Verify adapter classification allows a data commit kind from another lake format.
     */
    @Test
    void classifyAllowsFormatNeutralExternalDataSnapshot() {
        LakehouseEvent event = event("DELTA_COMMIT", Map.of(), List.of("dt=2026-09-01"));
        event.getPayloadJson().put(SnapshotEvidenceContract.DATA_CHANGE_FIELD, true);

        SnapshotTriggerRoutingService.SnapshotTriggerRoute route = routingService.classify(event);

        assertTrue(route.naturalProgressionAllowed());
        assertEquals("EXTERNAL", route.originType());
    }

    /**
     * Build a scheduling intent fixture.
     *
     * @param triggerType workflow trigger type
     * @return persisted intent fixture
     */
    private SchedulingIntent intent(String triggerType) {
        return SchedulingIntent.builder()
                .intentKey(INTENT_KEY)
                .triggerType(triggerType)
                .targetAssetKey(TARGET_ASSET)
                .bizDate(BIZ_DATE)
                .build();
    }

    /**
     * Build the properties a downstream writer must copy to its final snapshot.
     *
     * @param finalValue final commit marker
     * @return immutable property fixture
     */
    private Map<String, String> requiredProperties(String finalValue) {
        return Map.of(
                SnapshotEvidenceContract.SOURCE_PROPERTY, SnapshotEvidenceContract.INTENT_SOURCE,
                SnapshotEvidenceContract.INTENT_KEY_PROPERTY, INTENT_KEY,
                SnapshotEvidenceContract.TARGET_ASSET_PROPERTY, TARGET_ASSET,
                SnapshotEvidenceContract.BIZ_DATE_PROPERTY, "2026-09-01",
                SnapshotEvidenceContract.FINAL_PROPERTY, finalValue);
    }

    /**
     * Build one snapshot observation fixture.
     *
     * @param commitKind snapshot commit kind
     * @param properties snapshot attribution properties
     * @param changedPartitions changed partitions from delta manifests
     * @return event fixture
     */
    private LakehouseEvent event(
            String commitKind,
            Map<String, String> properties,
            List<String> changedPartitions) {
        return LakehouseEvent.builder()
                .eventId("event-101")
                .eventType("SNAPSHOT_COMMITTED")
                .sourceType("PAIMON")
                .catalogName("paimon")
                .databaseName("prod")
                .tableName("orders")
                .snapshotId("101")
                .commitKind(commitKind)
                .payloadJson(new java.util.LinkedHashMap<>(Map.of(
                        "snapshotProperties", properties,
                        "changedPartitions", changedPartitions,
                        SnapshotEvidenceContract.DATA_CHANGE_FIELD,
                        "APPEND".equals(commitKind) || "OVERWRITE".equals(commitKind))))
                .build();
    }
}
