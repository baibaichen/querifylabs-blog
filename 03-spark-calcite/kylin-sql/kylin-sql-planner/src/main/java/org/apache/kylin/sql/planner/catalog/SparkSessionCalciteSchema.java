package org.apache.kylin.sql.planner.catalog;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.Set;

/**
 * Bridge between the {@link SparkSession} and the {@link Schema}. This way we can query Spark's
 * specific catalogs from Calcite.
 *
 * <p>The mapping for {@link TableCatalog}s is modeled as a strict two-level reference structure for
 * Spark in Calcite, the full path of objects is of format
 * [catalog_name].[db_name].[meta-object_name].
 */

public class SparkSessionCalciteSchema extends SparkSchema {

    private final SparkSession session;

    public SparkSessionCalciteSchema(SparkSession session) {
        this.session = session;
    }

    @Override
    public Table getTable(String name) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return Collections.emptySet();
    }

    @Override
    public @Nullable Schema getSubSchema(String name) {
        final CatalogManager catalogManager = session.sessionState().catalogManager();

        if (!catalogManager.isCatalogRegistered(name)) {
            return null;
        }

        CatalogPlugin catalog = catalogManager.catalog(name);
        if (!(catalog instanceof TableCatalog)) {
            return null;
        }

        if (!(catalog instanceof SupportsNamespaces)) {
            return null;
        }

        return new SparkCatalogCalciteSchema(SparkCatalog.of(catalog));

    }

    /**
     * We currently only return "spark_catalog".
     * <p>
     * TODO: return all catalogs defined in configuration
     * @return all catalogs defined in configuration
     */
    @Override
    public Set<String> getSubSchemaNames() {
        return ImmutableSet.of(CatalogManager.SESSION_CATALOG_NAME());
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
        return null;
    }

    @Override
    public boolean isMutable() {
        return false;
    }
}
