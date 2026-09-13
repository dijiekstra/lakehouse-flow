package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSnapshotSource;
import io.github.lakehouseflow.integration.source.LakehouseSnapshotSourceProvider;
import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds one generic source per configured Paimon table.
 */
@Component
@EnableConfigurationProperties(PaimonSnapshotSourceProperties.class)
public class PaimonSnapshotSourceProvider implements LakehouseSnapshotSourceProvider {

    private static final String SOURCE_TYPE = "PAIMON";

    private final PaimonSnapshotSourceProperties properties;
    private final PaimonCatalogSnapshotReader snapshotReader;

    /**
     * Create the Paimon source provider.
     *
     * @param properties bound Paimon source configuration
     * @param snapshotReader native metadata reader shared by source adapters
     */
    public PaimonSnapshotSourceProvider(
            PaimonSnapshotSourceProperties properties,
            PaimonCatalogSnapshotReader snapshotReader) {
        this.properties = properties;
        this.snapshotReader = snapshotReader;
    }

    /**
     * Build configured Paimon sources without exposing Paimon SDK types to ingestion services.
     *
     * @return configured Paimon table sources
     */
    @Override
    public List<LakehouseSnapshotSource> sources() {
        if (!properties.isEnabled()) {
            return List.of();
        }
        ZoneId zoneId = parseZoneId(properties.getZoneId());
        List<LakehouseSnapshotSource> sources = new ArrayList<>();
        for (PaimonSnapshotSourceProperties.CatalogSource catalog : properties.getCatalogs()) {
            for (PaimonSnapshotSourceProperties.TableSource table : catalog.getTables()) {
                LakehouseSourceIdentity identity = identity(catalog, table);
                PaimonSourceDefinition definition = new PaimonSourceDefinition(
                        identity,
                        catalog.getOptions(),
                        table.getBatchSize(),
                        zoneId,
                        table.getStartupMode());
                sources.add(new PaimonSnapshotSource(definition, snapshotReader));
            }
        }
        return List.copyOf(sources);
    }

    /**
     * Build the stable common identity for one configured Paimon table.
     *
     * @param catalog catalog configuration
     * @param table table configuration
     * @return validated source identity
     */
    private LakehouseSourceIdentity identity(
            PaimonSnapshotSourceProperties.CatalogSource catalog,
            PaimonSnapshotSourceProperties.TableSource table) {
        String defaultSourceName = "%s.%s.%s".formatted(
                catalog.getName(), table.getDatabase(), table.getTable());
        String sourceName = table.getSourceName() == null || table.getSourceName().isBlank()
                ? defaultSourceName
                : table.getSourceName();
        return new LakehouseSourceIdentity(
                SOURCE_TYPE,
                sourceName,
                catalog.getName(),
                table.getDatabase(),
                table.getTable());
    }

    /**
     * Parse the event-model time zone and report configuration errors clearly.
     *
     * @param zoneId configured zone identifier
     * @return parsed time zone
     */
    private ZoneId parseZoneId(String zoneId) {
        try {
            return ZoneId.of(zoneId);
        } catch (DateTimeException | NullPointerException e) {
            throw new IllegalArgumentException("Invalid Paimon source zoneId: " + zoneId, e);
        }
    }
}
