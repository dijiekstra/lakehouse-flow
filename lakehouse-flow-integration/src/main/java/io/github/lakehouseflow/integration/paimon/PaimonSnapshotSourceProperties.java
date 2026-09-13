package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.SnapshotSourceStartupMode;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for Paimon catalogs and the table snapshots observed in each catalog.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "lakehouse-flow.snapshot-sources.paimon")
public class PaimonSnapshotSourceProperties {

    /** Whether the Paimon adapter contributes sources to the generic scanner. */
    private boolean enabled;

    /** Time zone used when epoch-based Paimon times enter the local-date-time event model. */
    private String zoneId = ZoneId.systemDefault().getId();

    /** Independently configured Paimon catalogs. */
    private List<CatalogSource> catalogs = new ArrayList<>();

    /**
     * Configuration of one Paimon catalog and its observed tables.
     */
    @Getter
    @Setter
    public static class CatalogSource {

        /** Logical catalog name used in Lakehouse Flow asset keys. */
        private String name;

        /** Native Paimon catalog options, including warehouse and metastore settings. */
        private Map<String, String> options = new LinkedHashMap<>();

        /** Tables whose snapshot histories are observed. */
        private List<TableSource> tables = new ArrayList<>();
    }

    /**
     * Configuration of one observed Paimon table.
     */
    @Getter
    @Setter
    public static class TableSource {

        /** Paimon database name. */
        private String database;

        /** Paimon table name. */
        private String table;

        /** Optional offset identity override; defaults to catalog.database.table. */
        private String sourceName;

        /** Maximum number of snapshots returned by one metadata scan. */
        private int batchSize = 100;

        /** Initial scan position; latest avoids accidental historical scheduling on onboarding. */
        private SnapshotSourceStartupMode startupMode = SnapshotSourceStartupMode.LATEST;
    }
}
