package io.apache.kylin.calcite.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import org.apache.spark.sql.connector.catalog.Table;

import java.util.Optional;

public class SparkSchemaTable extends AbstractTable {

    private final Table sparkTable;

    public SparkSchemaTable(Table sparkTable) {
        this.sparkTable = sparkTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }

    @Override
    public <C> C unwrapOrThrow(Class<C> aClass) {
        return super.unwrapOrThrow(aClass);
    }

    @Override
    public <C> Optional<C> maybeUnwrap(Class<C> aClass) {
        return super.maybeUnwrap(aClass);
    }
}
