package io.apache.kylin.planner;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptLattice;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.apache.kylin.planner.rules.KylinRules.KYLIN_RULES;

@Slf4j
public class Optimizer {
    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final VolcanoPlanner planner;
    private final List<RelOptLattice> lattices;

    public Optimizer(
      CalciteConnectionConfig config,
      SqlValidator validator,
      SqlToRelConverter converter,
      VolcanoPlanner planner,
      List<RelOptLattice> lattices) {
        this.config = config;
        this.validator = validator;
        this.converter = converter;
        this.planner = planner;
        this.lattices = lattices;
    }
    public static Optimizer of(String name, Schema schema) {
        return of(name, schema, ImmutableList.of());
    }

    public static Optimizer of(
      String name,
      Schema schema,
      List<Lattice> lattices) {
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        SchemaPlus root = rootSchema.plus();
        SchemaPlus defaultSchema = root.add(name, schema);

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(rootSchema,
          CalciteSchema.from(defaultSchema).path(null),
          typeFactory,
          config);

        SqlOperatorTable operatorTable = SqlOperatorTables.chain(SqlStdOperatorTable.instance());

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
          .withLenientOperatorLookup(config.lenientOperatorLookup())
          .withSqlConformance(config.conformance())
          .withDefaultNullCollation(config.defaultNullCollation())
          .withIdentifierExpansion(true);

        SqlValidator validator =
          SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptUtil.registerDefaultRules(planner, false, false);
        KYLIN_RULES.forEach(planner::addRule);
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
          converterConfig);

        List<RelOptLattice> relOptLattices =
          Util.transform(lattices,
            lattice -> new RelOptLattice(lattice, RelOptKylinTable.of(lattice, catalogReader, typeFactory)));
        return new Optimizer(config, validator, converter, planner, relOptLattices);
    }

    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser.Config config1 = SqlParser.config()
          .withCaseSensitive(config.caseSensitive())
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
        return program.run(planner, node, requiredTraitSet, Collections.emptyList(), Collections.emptyList());
    }

    public RelNode optimize(
      RelNode node,
      RelTraitSet requiredTraitSet,
      boolean dumpGraphviz) {
        Program program = Programs.standard();
        RelNode optimized = program.run(planner, node, requiredTraitSet, Collections.emptyList(), lattices);
        if (dumpGraphviz) {
            StringWriter sw = new StringWriter();
            DumperWrapper.dumpGraphviz(planner, new PrintWriter(sw));
            log.info("\n" + sw);
        }
        return optimized;
    }
}
