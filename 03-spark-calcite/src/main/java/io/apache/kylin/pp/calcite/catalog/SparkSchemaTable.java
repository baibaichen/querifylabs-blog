package io.apache.kylin.pp.calcite.catalog;

import com.google.common.base.Suppliers;
import io.apache.kylin.pp.calcite.impl.SparkTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;

import org.apache.spark.sql.connector.catalog.Table;

import java.util.Optional;
import java.util.function.Supplier;


public class SparkSchemaTable extends AbstractTable {

    private final Supplier<RelProtoDataType> protoRowTypeSupplier =
            Suppliers.memoize(this::supplyProto);

    private final Table sparkTable;

    public SparkSchemaTable(Table sparkTable) {
        this.sparkTable = sparkTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return protoRowTypeSupplier.get().apply(typeFactory);
    }

    private RelProtoDataType supplyProto() {
        // Temporary type factory, just for the duration of this method. Allowable
        // because we're creating a proto-type, not a type; before being used, the
        // proto-type will be copied into a real type factory.
        SparkTypeFactory sparkTypeFactory = new SparkTypeFactory();
        return RelDataTypeImpl.proto(sparkTypeFactory.buildRelNodeRowType(sparkTable.schema().fields()));
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
