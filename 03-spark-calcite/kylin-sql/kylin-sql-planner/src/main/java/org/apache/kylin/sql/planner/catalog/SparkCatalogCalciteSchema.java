package org.apache.kylin.sql.planner.catalog;

import org.apache.kylin.KylinSQLException;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A mapping between Spark's {@link SparkCatalog} and Calcite's schema. This enables to look up and access
 * objects(tables, views, functions, types) in SQL queries without registering them in advance.
 * Databases are registered as sub-schemas in the schema.
 */

public class SparkCatalogCalciteSchema extends SparkSchema {

    private final SparkCatalog catalog;

    public SparkCatalogCalciteSchema(SparkCatalog catalog) {
        this.catalog = catalog;
    }

    /**
     * Look up a sub-schema (database) by the given sub-schema name.
     *
     * @param schemaName name of sub-schema to look up
     * @return the sub-schema with a given database name, or null
     */
    @Override
    public Schema getSubSchema(String schemaName) {
        if (catalog.namespaceExists(new String[] { schemaName })) {
            return new TableCatalogCalciteSchema(schemaName, catalog);
        } else {
            return null;
        }
    }


    @Override
    public @Nullable Table getTable(String name) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return new HashSet<>();
    }

    @Override
    public Set<String> getSubSchemaNames() {
        try {
            return Arrays.stream(catalog.listNamespaces())
                    .map(x -> x[0]).collect(Collectors.toSet());
        } catch (NoSuchNamespaceException e) {
            throw KylinSQLException.error(KylinSQLException.ErrorCode.PARSING, e);
        }
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    @Override
    public boolean isMutable() {
        return false;
    }
}
