package io.github.lakehouseflow.integration.source;

import java.util.List;

/**
 * Supplies configured table sources for one lake format or metadata transport.
 */
public interface LakehouseSnapshotSourceProvider {

    /**
     * Build the currently configured snapshot sources.
     *
     * @return configured sources, or an empty list when the adapter is disabled
     */
    List<LakehouseSnapshotSource> sources();
}
