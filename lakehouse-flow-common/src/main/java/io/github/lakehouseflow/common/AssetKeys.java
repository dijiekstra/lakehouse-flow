package io.github.lakehouseflow.common;

import java.util.Optional;

/**
 * Format-neutral helpers for catalog-qualified table and partition asset keys.
 */
public final class AssetKeys {

    /** Prevent construction of this utility class. */
    private AssetKeys() {
    }

    /**
     * Resolve a table-level key from catalog.database.table[.partition].
     *
     * @param assetKey table or partition asset key
     * @return normalized table key when the input has at least three components
     */
    public static Optional<String> tableKey(String assetKey) {
        if (assetKey == null || assetKey.isBlank()) {
            return Optional.empty();
        }
        String[] parts = assetKey.trim().split("\\.", 4);
        if (parts.length < 3 || parts[0].isBlank() || parts[1].isBlank() || parts[2].isBlank()) {
            return Optional.empty();
        }
        return Optional.of("%s.%s.%s".formatted(parts[0], parts[1], parts[2]));
    }

    /**
     * Extract the optional partition suffix from an asset key.
     *
     * @param assetKey table or partition asset key
     * @return partition suffix when present
     */
    public static Optional<String> partition(String assetKey) {
        if (assetKey == null || assetKey.isBlank()) {
            return Optional.empty();
        }
        String[] parts = assetKey.trim().split("\\.", 4);
        return parts.length == 4 && !parts[3].isBlank()
                ? Optional.of(parts[3])
                : Optional.empty();
    }
}
