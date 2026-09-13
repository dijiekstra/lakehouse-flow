package io.github.lakehouseflow.integration.paimon;

import io.github.lakehouseflow.integration.source.LakehouseSourceIdentity;
import io.github.lakehouseflow.integration.source.SnapshotSourceStartupMode;

import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Validated immutable runtime definition of one Paimon table source.
 *
 * @param identity scheduler source and asset identity
 * @param catalogOptions native options passed to the Paimon Catalog factory
 * @param batchSize maximum snapshots read per scan
 * @param zoneId time zone used for epoch-to-local-time conversion
 * @param startupMode initial position when no durable offset exists
 */
public record PaimonSourceDefinition(
        LakehouseSourceIdentity identity,
        Map<String, String> catalogOptions,
        int batchSize,
        ZoneId zoneId,
        SnapshotSourceStartupMode startupMode) {

    /**
     * Validate source limits and defensively copy catalog options.
     */
    public PaimonSourceDefinition {
        if (identity == null) {
            throw new IllegalArgumentException("identity must not be null");
        }
        if (batchSize <= 0 || batchSize > 10_000) {
            throw new IllegalArgumentException("batchSize must be between 1 and 10000");
        }
        if (zoneId == null) {
            throw new IllegalArgumentException("zoneId must not be null");
        }
        if (startupMode == null) {
            throw new IllegalArgumentException("startupMode must not be null");
        }
        catalogOptions = catalogOptions == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(catalogOptions));
    }
}
