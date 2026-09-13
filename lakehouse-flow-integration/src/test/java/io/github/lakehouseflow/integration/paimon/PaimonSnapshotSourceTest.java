package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSnapshot;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceStartupMode;
import io.github.lakehouseflow.integration.source.SnapshotSourceOffsetStatus;
import io.github.lakehouseflow.integration.source.SnapshotSourcePosition;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the thin Paimon implementation of the generic source SPI.
 */
class PaimonSnapshotSourceTest {

    /**
     * Verify identity, scan delegation, and numeric offset ordering.
     */
    @Test
    void sourceImplementsGenericContract() {
        LakehouseSourceIdentity identity = new LakehouseSourceIdentity(
                "PAIMON", "catalog.db.orders", "catalog", "db", "orders");
        PaimonSourceDefinition definition = new PaimonSourceDefinition(
                identity,
                Map.of("warehouse", "file:///tmp/warehouse"),
                100,
                ZoneId.of("UTC"),
                SnapshotSourceStartupMode.LATEST);
        PaimonCatalogSnapshotReader reader = mock(PaimonCatalogSnapshotReader.class);
        LakehouseSnapshot snapshot = new LakehouseSnapshot(
                "10", "10", "1", null, "APPEND", true, null, null, null, null);
        when(reader.scanAfter(definition, "9")).thenReturn(List.of(snapshot));
        SnapshotSourcePosition position = new SnapshotSourcePosition(
                SnapshotSourceOffsetStatus.LAGGING,
                "9", "1", "10", "10", 1L, "lagging");
        when(reader.inspectPosition(definition, "9")).thenReturn(position);
        PaimonSnapshotSource source = new PaimonSnapshotSource(definition, reader);

        assertEquals(identity, source.identity());
        assertEquals(List.of(snapshot), source.scanAfter("9"));
        assertEquals(position, source.inspectPosition("9"));
        assertEquals(-1, Integer.signum(source.offsetComparator().compare("2", "10")));
        verify(reader).scanAfter(definition, "9");
        verify(reader).inspectPosition(definition, "9");
    }
}
