package io.apache.kylin.spark.calcite;

import com.google.common.collect.ImmutableList;
import io.apache.kylin.calcite.catalog.SparkSessionCalciteSchema;
import io.apache.kylin.calcite.impl.CalciteConfig;
import io.apache.kylin.calcite.impl.SparkTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.spark.sql.SparkSession;



/**
 * Utility class to create {@link org.apache.calcite.tools.RelBuilder} or {@link FrameworkConfig}
 * used to create a corresponding {@link org.apache.calcite.tools.Planner}. It tries to separate
 * static elements in a {@link TableEnvironment} like: root schema, cost factory, type system etc.
 * from a dynamic properties like e.g. default path to look for objects in the schema.
 *
 * <p/>
 * Make it thread safe ?
 */
public class PlannerContext {
    private final CalciteSchema rootSchema;
    private final SparkTypeFactory typeFactory;

    public PlannerContext(SparkSession session) {
        this.rootSchema = CalciteSchemaBuilder.asRootSchema(new SparkSessionCalciteSchema(session));
        typeFactory = new SparkTypeFactory();
    }

    private static SchemaPlus getRootSchema(SchemaPlus schema) {
        for (;;) {
            SchemaPlus parentSchema = schema.getParentSchema();
            if (parentSchema == null) {
                return schema;
            }
            schema = parentSchema;
        }
    }

    public CalciteCatalogReader createCatalogReader() {
        SchemaPlus rootSchema = getRootSchema(this.rootSchema.plus());
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                ImmutableList.of(),
                typeFactory,
                CalciteConfig.DEFAULT.toConnectionConfig());
    }
    public SqlValidator createSqlValidator(Prepare.CatalogReader catalogReader){
        // less flexible
        final SqlOperatorTable opTab =
                SqlOperatorTables.chain(SqlStdOperatorTable.instance(), catalogReader);

        return SqlValidatorUtil.newValidator(opTab, catalogReader, typeFactory, CalciteConfig.DEFAULT_VALIDATOR_CONFIG);
    }
}
