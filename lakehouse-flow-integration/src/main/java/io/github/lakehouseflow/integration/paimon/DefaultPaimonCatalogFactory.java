package io.github.lakehouseflow.integration.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Production factory backed by the Apache Paimon Catalog API.
 */
@Component
public class DefaultPaimonCatalogFactory implements PaimonCatalogFactory {

    /**
     * Open the catalog selected by Paimon's native metastore options.
     *
     * @param catalogOptions native catalog options
     * @return open Paimon catalog
     */
    @Override
    public Catalog openCatalog(Map<String, String> catalogOptions) {
        Options options = Options.fromMap(catalogOptions);
        return org.apache.paimon.catalog.CatalogFactory.createCatalog(CatalogContext.create(options));
    }
}
