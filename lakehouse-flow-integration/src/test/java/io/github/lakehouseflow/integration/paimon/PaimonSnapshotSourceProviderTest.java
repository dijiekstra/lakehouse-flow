package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSnapshotSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Tests expansion of Paimon catalog configuration into generic table sources.
 */
class PaimonSnapshotSourceProviderTest {

    /**
     * Verify a disabled adapter contributes no source.
     */
    @Test
    void sourcesReturnsEmptyWhenDisabled() {
        PaimonSnapshotSourceProperties properties = new PaimonSnapshotSourceProperties();
        PaimonSnapshotSourceProvider provider = new PaimonSnapshotSourceProvider(
                properties, mock(PaimonCatalogSnapshotReader.class));

        assertEquals(List.of(), provider.sources());
    }

    /**
     * Verify each configured table becomes an independently offset source.
     */
    @Test
    void sourcesBuildsOneSourcePerTable() {
        PaimonSnapshotSourceProperties properties = configuredProperties();
        PaimonSnapshotSourceProvider provider = new PaimonSnapshotSourceProvider(
                properties, mock(PaimonCatalogSnapshotReader.class));

        List<LakehouseSnapshotSource> sources = provider.sources();

        assertEquals(1, sources.size());
        assertEquals("PAIMON", sources.get(0).identity().sourceType());
        assertEquals("lake.ods.orders", sources.get(0).identity().sourceName());
        assertEquals("lake.ods.orders", sources.get(0).identity().assetKey());
    }

    /**
     * Verify invalid time-zone configuration fails before any metadata scan.
     */
    @Test
    void sourcesRejectsInvalidZoneId() {
        PaimonSnapshotSourceProperties properties = configuredProperties();
        properties.setZoneId("Mars/Olympus");
        PaimonSnapshotSourceProvider provider = new PaimonSnapshotSourceProvider(
                properties, mock(PaimonCatalogSnapshotReader.class));

        assertThrows(IllegalArgumentException.class, provider::sources);
    }

    private PaimonSnapshotSourceProperties configuredProperties() {
        PaimonSnapshotSourceProperties.TableSource table = new PaimonSnapshotSourceProperties.TableSource();
        table.setDatabase("ods");
        table.setTable("orders");

        PaimonSnapshotSourceProperties.CatalogSource catalog =
                new PaimonSnapshotSourceProperties.CatalogSource();
        catalog.setName("lake");
        catalog.setOptions(Map.of("warehouse", "file:///tmp/warehouse"));
        catalog.setTables(List.of(table));

        PaimonSnapshotSourceProperties properties = new PaimonSnapshotSourceProperties();
        properties.setEnabled(true);
        properties.setZoneId("UTC");
        properties.setCatalogs(List.of(catalog));
        return properties;
    }
}
