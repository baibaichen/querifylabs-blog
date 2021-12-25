package io.apache.kylin.calcite.planner;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.DumperWrapper;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Properties;

public class Optimizer {

    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final VolcanoPlanner planner;

    public Optimizer(
        CalciteConnectionConfig config,
        SqlValidator validator,
        SqlToRelConverter converter,
        VolcanoPlanner planner
    ) {
        this.config = config;
        this.validator = validator;
        this.converter = converter;
        this.planner = planner;
    }

    public static Optimizer create(String name, Schema schema) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(name, schema);
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(name),
            typeFactory,
            config
        );

        SqlOperatorTable operatorTable = SqlOperatorTables.chain(SqlStdOperatorTable.instance());

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
            .withLenientOperatorLookup(config.lenientOperatorLookup())
            .withSqlConformance(config.conformance())
            .withDefaultNullCollation(config.defaultNullCollation())
            .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptUtil.registerDefaultRules(planner, false, false);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            .withExpand(false); // https://issues.apache.org/jira/browse/CALCITE-1045

        SqlToRelConverter converter = new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig
        );

        return new Optimizer(config, validator, converter, planner);
    }

    public SqlNode parse(String sql) throws Exception {
        SqlParser.Config config1 = SqlParser.config().withCaseSensitive(config.caseSensitive())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuotedCasing(config.quotedCasing())
                .withConformance(config.conformance());

        SqlParser parser = SqlParser.create(sql, config1);

        return parser.parseStmt();
    }

    public SqlNode validate(SqlNode node) {
        return validator.validate(node);
    }

    public RelNode convert(SqlNode node) {
        RelRoot root = converter.convertQuery(node, false, true);

        return root.rel;
    }

    public RelNode beforeOptimize(RelNode node, RelTraitSet requiredTraitSet) {
        Program program = Programs.sequence(Programs.subQuery(DefaultRelMetadataProvider.INSTANCE));
        return program.run(
                planner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList());
    }

    public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, boolean dumpGraphviz) {
        Program program = Programs.standard();

        RelNode optimized = program.run(
            planner,
            node,
            requiredTraitSet,
            Collections.emptyList(),
            Collections.emptyList()
        );

        if (dumpGraphviz) {
            StringWriter sw = new StringWriter();
            DumperWrapper.dumpGraphviz(planner, new PrintWriter(sw));
            System.out.println(sw);
        }
        return optimized;
    }
}
