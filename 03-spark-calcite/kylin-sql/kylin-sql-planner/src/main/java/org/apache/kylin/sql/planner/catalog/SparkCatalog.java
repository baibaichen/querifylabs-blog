package org.apache.kylin.sql.planner.catalog;

import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;

public class SparkCatalog extends DelegatingCatalogExtension {

    public static SparkCatalog of(CatalogPlugin delegate) {
        SparkCatalog result = new SparkCatalog();
        result.setDelegateCatalog(delegate);
        return result;
    }
}
