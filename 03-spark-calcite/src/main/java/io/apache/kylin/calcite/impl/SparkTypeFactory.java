package io.apache.kylin.calcite.impl;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;

/**
 * Spark specific type factory that represents the interface between Spark's {@link org.apache.spark.sql.types.DataType DataType}
 * and Calcite's {@link org.apache.calcite.rel.type.RelDataType RelDataType}.
 */
public class SparkTypeFactory extends JavaTypeFactoryImpl {

    public SparkTypeFactory() {
        super(SparkTypeSystem.INSTANCE);
    }

    public SparkTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }
}
