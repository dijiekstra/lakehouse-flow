package io.github.lakehouseflow.integration.paimon;

import org.apache.paimon.catalog.Catalog;

import java.util.Map;

/**
 * Opens a Paimon catalog while keeping SDK construction mockable in unit tests.
 */
public interface PaimonCatalogFactory {

    /**
     * Open a Paimon catalog for one metadata scan.
     *
     * @param catalogOptions native Paimon catalog options
     * @return open catalog owned by the caller
     */
    Catalog openCatalog(Map<String, String> catalogOptions);
}
