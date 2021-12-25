package io.apache.kylin.planner;

import com.google.common.collect.ImmutableList;
import io.apache.kylin.test.Resource.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.CalciteAssert.SchemaSpec;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
class RewriteTest {

    protected Sql sql(String materialize, String query) {
        return ImmutableSql.of(query, this)
                .withMaterializations(ImmutableList.of(Pair.of(materialize, "MV0")));
    }

    @Test
    void testUsingPlanner() throws RelConversionException, SqlParseException, ValidationException {
        Planner planner = Util.getPlanner(null, SqlParser.Config.DEFAULT, Programs.standard());
        SqlNode parse = planner.parse("select count(*) as c from \"emps\" group by \"empid\"");
        SqlNode validate = planner.validate(parse);
        RelNode convert = planner.rel(validate).rel;
        RelTraitSet traitSet = convert.getTraitSet()
                .replace(EnumerableConvention.INSTANCE);
        RelNode transform = planner.transform(0, traitSet, convert);
        log.info("\n" + Util.toString(transform));
    }
    /** Aggregation query at same level of aggregation as aggregation
     * materialization. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false}) // six numbers
    void testAggregate0(boolean useSubstitution) {
        sql("select count(*) as c, sum(\"salary\"+1) as d from \"emps\" group by \"empid\"",
                "select sum(\"salary\"+1) as c from \"emps\" group by \"empid\"")
                .withSubstitution(useSubstitution)
                .ok();
    }
    private void checkMaterialize(Sql sql) {
        final TestConfig testConfig = build(sql);
        List<RelNode> substitutes;
        if (sql.getSubstitution()) {
            substitutes = optimize1(testConfig);
        } else {
            substitutes = optimize2(testConfig);
        }
        assert substitutes != null;
        substitutes.forEach(relNode -> log.info("\n" + Util.toString(relNode)));
    }

    List<RelNode> optimize2(TestConfig testConfig) {
        RelNode queryRel = testConfig.queryRel;
        RelOptPlanner planner = queryRel.getCluster().getPlanner();
        RelTraitSet traitSet = queryRel.getCluster().traitSet()
                .replace(EnumerableConvention.INSTANCE);
        RelOptUtil.registerDefaultRules(planner, true, false);
        return ImmutableList.of(
                Programs.standard().run(
                        planner, queryRel, traitSet, testConfig.materializations, ImmutableList.of()));
    }

    protected List<RelNode> optimize1(TestConfig testConfig) {
        RelNode queryRel = testConfig.queryRel;
        RelOptMaterialization materialization = testConfig.materializations.get(0);
        return new SubstitutionVisitor(canonicalize(materialization.queryRel), canonicalize(queryRel))
                .go(materialization.tableRel);
    }
    private RelNode canonicalize(RelNode rel) {
        HepProgram program =
                new HepProgramBuilder()
                        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
                        .addRuleInstance(CoreRules.FILTER_MERGE)
                        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
                        .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
                        .addRuleInstance(CoreRules.PROJECT_MERGE)
                        .addRuleInstance(CoreRules.PROJECT_REMOVE)
                        .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
                        .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
                        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS)
                        .addRuleInstance(CoreRules.FILTER_TO_CALC)
                        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
                        .addRuleInstance(CoreRules.FILTER_CALC_MERGE)
                        .addRuleInstance(CoreRules.PROJECT_CALC_MERGE)
                        .addRuleInstance(CoreRules.CALC_MERGE)
                        .build();
        final HepPlanner hepPlanner = new HepPlanner(program);
        hepPlanner.setRoot(rel);
        return hepPlanner.findBestExp();
    }

    private TestConfig build(Sql sql) {
        assert sql != null;
        return Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
            cluster.getPlanner().setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
            try {
                final SchemaPlus defaultSchema;
                if (sql.getDefaultSchemaSpec() == null) {
                    defaultSchema = rootSchema.add("hr", new ReflectiveSchema(new Util.HrFKUKSchema()));
                } else {
                    defaultSchema = CalciteAssert.addSchema(rootSchema, sql.getDefaultSchemaSpec());
                }
                final RelNode queryRel = toRel(cluster, rootSchema, defaultSchema, sql.getQuery());
                final List<RelOptMaterialization> mvs = new ArrayList<>();
                final RelBuilder relBuilder =
                        RelFactories.LOGICAL_BUILDER.create(cluster, relOptSchema);
                final MaterializationService.DefaultTableFactory tableFactory =
                        new MaterializationService.DefaultTableFactory();
                for (Pair<String, String> pair: sql.getMaterializations()) {
                    final RelNode mvRel = toRel(cluster, rootSchema, defaultSchema, pair.left);
                    final Table table = tableFactory.createTable(CalciteSchema.from(rootSchema),
                            pair.left, ImmutableList.of(defaultSchema.getName()));
                    defaultSchema.add(pair.right, table);
                    relBuilder.scan(defaultSchema.getName(), pair.right);
                    final LogicalTableScan logicalScan = (LogicalTableScan) relBuilder.build();
                    final EnumerableTableScan replacement =
                            EnumerableTableScan.create(cluster, logicalScan.getTable());
                    assert pair.right != null;
                    mvs.add(
                            new RelOptMaterialization(replacement, mvRel, null,
                                    ImmutableList.of(defaultSchema.getName(), pair.right)));
                }
                return new TestConfig(defaultSchema.getName(), queryRel, mvs);
            } catch (Exception e) {
                throw TestUtil.rethrow(e);
            }
        });
    }

    private RelNode toRel(RelOptCluster cluster, SchemaPlus rootSchema,
                          SchemaPlus defaultSchema, String sql) throws SqlParseException {
        final SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        final SqlNode parsed = parser.parseStmt();

        final CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(defaultSchema).path(null),
                new JavaTypeFactoryImpl(),
                CalciteConnectionConfig.DEFAULT);

        final SqlValidator validator = new ValidatorForTest(SqlStdOperatorTable.instance(),
                catalogReader, new JavaTypeFactoryImpl(), SqlConformanceEnum.DEFAULT);
        final SqlNode validated = validator.validate(parsed);
        final SqlToRelConverter.Config config = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(true)
                .withDecorrelationEnabled(true);
        final SqlToRelConverter converter = new SqlToRelConverter(
                (rowType, queryString, schemaPath, viewPath) -> {
                    throw new UnsupportedOperationException("cannot expand view");
                }, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);
        return converter.convertQuery(validated, false, true).rel;
    }

    /** Validator for testing. */
    private static class ValidatorForTest extends SqlValidatorImpl {
        ValidatorForTest(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
                         RelDataTypeFactory typeFactory, SqlConformance conformance) {
            super(opTab, catalogReader, typeFactory, Config.DEFAULT.withSqlConformance(conformance));
        }
    }

    protected static class TestConfig {
        public final String defaultSchema;
        public final RelNode queryRel;
        public final List<RelOptMaterialization> materializations;

        public TestConfig(String defaultSchema, RelNode queryRel,
                          List<RelOptMaterialization> materializations) {
            this.defaultSchema = defaultSchema;
            this.queryRel = queryRel;
            this.materializations = materializations;
        }
    }

    @Value.Immutable()
    public interface Sql {

        default void ok() {
            getTester().checkMaterialize(this);
        }

        default void noMat() {
            getTester().checkMaterialize(this);
        }

        @Nullable SchemaSpec getDefaultSchemaSpec();
        Sql withDefaultSchemaSpec(@Nullable SchemaSpec spec);

        List<Pair<String, String>> getMaterializations();
        Sql withMaterializations(Iterable<? extends Pair<String, String>> materialize);

        @Value.Parameter
        String getQuery();
        Sql withQuery(String query);

        @Nullable Function<String, Boolean> getChecker();
        Sql withChecker(@Nullable Function<String, Boolean> checker);

        @Value.Parameter
        RewriteTest getTester();
        Sql withTester(RewriteTest tester);

        @Value.Default
        default boolean getSubstitution() {
            return true;
        }
        Sql withSubstitution(boolean useSubstitution);
    }
}
