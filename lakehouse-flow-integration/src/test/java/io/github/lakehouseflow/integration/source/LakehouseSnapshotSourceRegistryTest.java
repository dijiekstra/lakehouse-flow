package io.github.lakehouseflow.integration.source;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests configured source identity isolation across providers.
 */
class LakehouseSnapshotSourceRegistryTest {

    /** Verify unique configured sources are returned unchanged. */
    @Test
    void sourcesReturnsUniqueSources() {
        LakehouseSnapshotSource source = source(
                new LakehouseSourceIdentity("PAIMON", "orders", "lake", "ods", "orders"));
        LakehouseSnapshotSourceProvider provider = mock(LakehouseSnapshotSourceProvider.class);
        when(provider.sources()).thenReturn(List.of(source));

        assertEquals(List.of(source), new LakehouseSnapshotSourceRegistry(List.of(provider)).sources());
    }

    /** Verify duplicate offset identities are rejected before scanning. */
    @Test
    void sourcesRejectsDuplicateOffsetIdentity() {
        LakehouseSourceIdentity identity =
                new LakehouseSourceIdentity("PAIMON", "orders", "lake", "ods", "orders");
        LakehouseSnapshotSourceProvider provider = mock(LakehouseSnapshotSourceProvider.class);
        LakehouseSnapshotSource first = source(identity);
        LakehouseSnapshotSource duplicate = source(identity);
        when(provider.sources()).thenReturn(List.of(first, duplicate));

        assertThrows(
                IllegalStateException.class,
                () -> new LakehouseSnapshotSourceRegistry(List.of(provider)).sources());
    }

    /** Build one source mock with a stable identity. */
    private LakehouseSnapshotSource source(LakehouseSourceIdentity identity) {
        LakehouseSnapshotSource source = mock(LakehouseSnapshotSource.class);
        when(source.identity()).thenReturn(identity);
        return source;
    }
}
