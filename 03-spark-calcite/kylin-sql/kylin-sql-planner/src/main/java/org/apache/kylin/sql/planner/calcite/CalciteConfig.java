package org.apache.kylin.sql.planner.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.Properties;

/**
 * Configuration passed to the Calcite.
 */
public class CalciteConfig {

    /**
     * Whether to expand subqueries. When set to {@code false}, subqueries are left as is in the form of
     * {@link org.apache.calcite.rex.RexSubQuery}. Otherwise they are expanded into {@link org.apache.calcite.rel.core.Correlate}
     * instances.
     * Do not enable this because you may run into <a href="https://issues.apache.org/jira/browse/CALCITE-3484">CALCITE-3484</a>. Instead, subquery
     * elimination rules are executed during logical planning. In addition, resulting plans are slightly better that those
     * produced by "expand" flag.
     */
    private static final boolean EXPAND = false;

    /**
     * currently SqlToRelConverter creates not optimal plan for both optimization and execution.
     * so it's better to disable such rewriting right now.
     *
     * <p/>
     * See <a href="https://issues.apache.org/jira/browse/IGNITE-14277">Calcite. Rewrite IN predicate to semi-join</a>
     */
    private static final int KYLIN_IN_ELEMENTS_THRESHOLD = Integer.MAX_VALUE;

    /**
     * Whether to trim unused fields. The trimming is needed after subquery elimination.
     */
    private static final boolean TRIM_UNUSED_FIELDS = false;


    public static final CalciteConfig DEFAULT =
            new CalciteConfig(false, Casing.UNCHANGED, Casing.UNCHANGED, Quoting.DOUBLE_QUOTE);

    public static final SqlParser.Config DEFAULT_PARSER_CONFIG =
            DEFAULT.toParserConfig(SqlParser.config()).withConformance(SqlConformanceEnum.DEFAULT);

    public static final SqlValidator.Config DEFAULT_VALIDATOR_CONFIG =
            DEFAULT.toValidatorConfig(SqlValidator.Config.DEFAULT).withSqlConformance(SqlConformanceEnum.DEFAULT);

    public static final SqlToRelConverter.Config DEFAULT_TO_REL_CONVERTER_CONFIG =
            DEFAULT.toSqlToRelConverterConfig(SqlToRelConverter.config());

    public static final CalciteConnectionConfig DEFAULT_CONNECTION_CONFIG = DEFAULT.toConnectionConfig();

    /**
     * default Framework config
     * <ul>
     *    <li> TODO: costFactory
     *    <li> TODO: operatorTable
     *    <li> TODO: executor
     *    <li> TODO: context
     *    <li> TODO: traitDefs
     * </ul>
     */
    public static final FrameworkConfig FRAMEWORK_CONFIG =
      Frameworks.newConfigBuilder()
        .parserConfig(DEFAULT_PARSER_CONFIG)
        .sqlValidatorConfig(DEFAULT_VALIDATOR_CONFIG)
        .sqlToRelConverterConfig(DEFAULT_TO_REL_CONVERTER_CONFIG)
        .typeSystem(SparkTypeSystem.INSTANCE)
        .build();

    private final boolean caseSensitive;
    private final Casing unquotedCasing;
    private final Casing quotedCasing;
    private final Quoting quoting;

    public CalciteConfig(boolean caseSensitive, Casing unquotedCasing, Casing quotedCasing, Quoting quoting) {
        this.caseSensitive = caseSensitive;
        this.unquotedCasing = unquotedCasing;
        this.quotedCasing = quotedCasing;
        this.quoting = quoting;
    }

    public SqlParser.Config toParserConfig(SqlParser.Config config) {
        return config
                .withCaseSensitive(caseSensitive)
                .withUnquotedCasing(unquotedCasing)
                .withQuotedCasing(quotedCasing)
                .withQuoting(quoting);
    }

    public SqlValidator.Config toValidatorConfig(SqlValidator.Config config){
        CalciteConnectionConfig connectionConfig = toConnectionConfig();
        return config
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                .withIdentifierExpansion(true);
    }

    public SqlToRelConverter.Config toSqlToRelConverterConfig(SqlToRelConverter.Config config) {
        // see Flink's PlannerContext#getSqlToRelConverterConfig
        // TODO: withHintStrategyTable
        // TODO: withRelBuilderFactory
        return config.withExpand(EXPAND)
                .withInSubQueryThreshold(KYLIN_IN_ELEMENTS_THRESHOLD)
                .withTrimUnusedFields(TRIM_UNUSED_FIELDS);
    }

    public CalciteConnectionConfig toConnectionConfig() {
        Properties connectionProperties = new Properties();
        connectionProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.toString(caseSensitive));
        connectionProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), unquotedCasing.toString());
        connectionProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), quotedCasing.toString());
        connectionProperties.put(CalciteConnectionProperty.QUOTING.camelName(), quoting.toString());

        // disable Substitution
        connectionProperties.put(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), Boolean.toString(false));
        return new CalciteConnectionConfigImpl(connectionProperties);
    }
}

