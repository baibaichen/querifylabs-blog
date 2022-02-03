package io.apache.kylin.pp.calcite.catalog;

import io.apache.kylin.pp.calcite.KylinSQLException;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TableCatalogCalciteSchema extends SparkSchema {
    private final String schemaName;
    private final TableCatalog tableCatalog;

    public TableCatalogCalciteSchema(String schemaName, TableCatalog tableCatalog) {
        this.schemaName = schemaName;
        this.tableCatalog = tableCatalog;
    }

    @Override
    public @Nullable Table getTable(String name) {
        Identifier id = Identifier.of(new String[]{schemaName}, name);
        try {
            if (tableCatalog.tableExists(id)) {
                return new SparkSchemaTable(tableCatalog.loadTable(id));
            }else {
                return null;
            }
        } catch(NoSuchTableException e) {
            throw new KylinSQLException(e);
        }
    }

    @Override
    public Set<String> getTableNames() {
        try {
            return Arrays.stream(tableCatalog.listTables(new String[]{schemaName}))
                    .map(Identifier::name)
                    .collect(Collectors.toSet());
        } catch (NoSuchNamespaceException e) {
            throw KylinSQLException.error(KylinSQLException.ErrorCode.PARSING, e);
        }
    }

    @Override
    public @Nullable Schema getSubSchema(String name) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return new HashSet<>();
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
