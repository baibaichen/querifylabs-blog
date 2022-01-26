package io.apache.kylin.calcite.impl;

import net.jcip.annotations.ThreadSafe;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;



/**
 * Custom type system for Spark.
 */
@ThreadSafe
public class SparkTypeSystem extends RelDataTypeSystemImpl {
    public static final RelDataTypeSystem INSTANCE = new SparkTypeSystem();
}
