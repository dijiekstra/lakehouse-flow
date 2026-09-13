package io.github.lakehouseflow.integration.source;

import java.util.Locale;
import java.util.Objects;

/**
 * Stable identity of one lakehouse table metadata source.
 *
 * @param sourceType lake format such as PAIMON, ICEBERG, or HUDI
 * @param sourceName source instance key used by the durable offset
 * @param catalogName logical catalog name used by scheduler asset keys
 * @param databaseName logical database or schema name
 * @param tableName logical table name
 */
public record LakehouseSourceIdentity(
        String sourceType,
        String sourceName,
        String catalogName,
        String databaseName,
        String tableName) {

    /**
     * Validate the immutable source identity at construction time.
     */
    public LakehouseSourceIdentity {
        sourceType = requireText(sourceType, "sourceType").toUpperCase(Locale.ROOT);
        sourceName = requireText(sourceName, "sourceName");
        catalogName = requireText(catalogName, "catalogName");
        databaseName = requireText(databaseName, "databaseName");
        tableName = requireText(tableName, "tableName");
    }

    /**
     * Return the table-level asset key used by scheduling definitions.
     *
     * @return catalog-qualified table asset key
     */
    public String assetKey() {
        return "%s.%s.%s".formatted(catalogName, databaseName, tableName);
    }

    /**
     * Normalize and validate one required identity component.
     *
     * @param value source component value
     * @param fieldName component name used in validation errors
     * @return trimmed component value
     */
    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return Objects.requireNonNull(value).trim();
    }
}
