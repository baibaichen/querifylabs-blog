package io.apache.kylin.calcite.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.spark.sql.types.StructField;

/**
 * Spark specific type factory that represents the interface between Spark's {@link org.apache.spark.sql.types.DataType DataType}
 * and Calcite's {@link org.apache.calcite.rel.type.RelDataType RelDataType}.
 */
@Slf4j
public class SparkTypeFactory extends JavaTypeFactoryImpl {

    public SparkTypeFactory() {
        super(SparkTypeSystem.INSTANCE);
    }

    public SparkTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    public void checkX(StructField field) {
      log.info(field.toString());
    }
}
