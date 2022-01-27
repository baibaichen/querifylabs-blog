package io.apache.kylin.calcite.impl;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Properties;

/**
 * Configuration passed to the Calcite.
 */
public class CalciteConfig {
    public static final CalciteConfig DEFAULT =
            new CalciteConfig(false, Casing.UNCHANGED, Casing.UNCHANGED, Quoting.DOUBLE_QUOTE);

    public static final SqlParser.Config DEFAULT_PARSER_CONFIG =
            DEFAULT.toParserConfig(SqlParser.config()).withConformance(SqlConformanceEnum.DEFAULT);

    public static final SqlValidator.Config DEFAULT_VALIDATOR_CONFIG =
            DEFAULT.toValidatorConfig(SqlValidator.Config.DEFAULT).withSqlConformance(SqlConformanceEnum.DEFAULT);

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

    public CalciteConnectionConfig toConnectionConfig() {
        Properties connectionProperties = new Properties();
        connectionProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.toString(caseSensitive));
        connectionProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), unquotedCasing.toString());
        connectionProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), quotedCasing.toString());
        connectionProperties.put(CalciteConnectionProperty.QUOTING.camelName(), quoting.toString());
        return new CalciteConnectionConfigImpl(connectionProperties);
    }
}

