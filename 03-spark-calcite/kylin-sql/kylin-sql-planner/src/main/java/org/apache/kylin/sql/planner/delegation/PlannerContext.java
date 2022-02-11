package org.apache.kylin.sql.planner.delegation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.kylin.sql.planner.calcite.CalciteConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.kylin.sql.planner.parse.CalciteParser;

import java.util.function.Supplier;

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
    private final RelDataTypeFactory typeFactory;

    private final Supplier<RelOptCluster> optClusterSupplier = Suppliers.memoize(this::createCluster);

    public PlannerContext(CalciteSchema rootSchema, RelDataTypeFactory typeFactory) {
        this.rootSchema = rootSchema;
        this.typeFactory = typeFactory;
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
        return new CalciteCatalogReader(
                CalciteSchema.from(getRootSchema(rootSchema.plus())),
                ImmutableList.of(),
                typeFactory,
                CalciteConfig.DEFAULT.toConnectionConfig());
    }

    private SqlValidator createSqlValidator() {
        return createSqlValidator(createCatalogReader());
    }
    private SqlValidator createSqlValidator(Prepare.CatalogReader catalogReader) {
        // less flexible
        final SqlOperatorTable opTab =
                SqlOperatorTables.chain(SqlStdOperatorTable.instance(), catalogReader);

        return SqlValidatorUtil.newValidator(opTab, catalogReader, typeFactory, CalciteConfig.DEFAULT_VALIDATOR_CONFIG);
    }

    public RelOptCluster createCluster() {
        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY,
                Contexts.of(CalciteConfig.DEFAULT_CONNECTION_CONFIG));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    public CalciteParser createParser() {
        return new CalciteParser(createSqlValidator(), optClusterSupplier.get());
    }

    @VisibleForTesting
    public CalciteSchema getRootSchema() {
        return rootSchema;
    }

    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public RelOptCluster getOptCluster() {
        return optClusterSupplier.get();
    }

    public VolcanoPlanner getPlanner() {
        return (VolcanoPlanner) getOptCluster().getPlanner();
    }
}
