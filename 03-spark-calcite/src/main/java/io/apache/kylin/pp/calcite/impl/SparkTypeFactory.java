package io.apache.kylin.pp.calcite.impl;

import com.google.common.collect.ImmutableMap;
import io.apache.kylin.pp.calcite.KylinSQLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.Map;

/**
 * Spark specific type factory that represents the interface between Spark's {@link org.apache.spark.sql.types.DataType DataType}
 * and Calcite's {@link org.apache.calcite.rel.type.RelDataType RelDataType}.
 */
@Slf4j
public class SparkTypeFactory extends JavaTypeFactoryImpl {

    private static final Map<DataType, SqlTypeName> SPARK_TYPE_TO_NAME =
            ImmutableMap.<DataType, SqlTypeName>builder()
                    .put(IntegerType$.MODULE$, SqlTypeName.INTEGER)
                    .put(StringType$.MODULE$, SqlTypeName.VARCHAR)
                    .build();

    public SparkTypeFactory() {
        super(SparkTypeSystem.INSTANCE);
    }

    public SparkTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    public RelDataType buildRelNodeRowType(StructField[] fields) {
        final RelDataTypeFactory.Builder fieldInfo = builder();
        fieldInfo.kind(StructKind.FULLY_QUALIFIED);
        Arrays.stream(fields).forEach(field -> {
            RelDataType fieldRelDataType = createFieldTypeFrom(field);
            checkForNullType(fieldRelDataType);
            fieldInfo.add(field.name(), fieldRelDataType).nullable(field.nullable());
        });
        return fieldInfo.build();
    }

    private RelDataType createFieldTypeFrom(StructField field) {
        SqlTypeName typeName = SPARK_TYPE_TO_NAME.get(field.dataType());
        if (typeName == null) {
            throw new UnsupportedOperationException(
                    String.format("Kylin doesn't support spark data type %s yet.", field.dataType()));
        }
        return createSqlType(typeName);
    }

    /**
     * This is a safety check in case the null type ends up in the type factory for other use cases
     * than untyped NULL literals.
     */
    private void checkForNullType(RelDataType... childTypes) {
        Arrays.stream(childTypes).forEach(childType -> {
            if (childType.getSqlTypeName() == SqlTypeName.NULL) {
                throw KylinSQLException.error(KylinSQLException.ErrorCode.VALIDATING,
                        "The null type is reserved for representing untyped NULL literals. It should not be " +
                                "used in constructed types. Please cast NULL literals to a more explicit type.");
            }
        });
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        return super.createSqlType(typeName);
    }
}
