package org.apache.kylin.sql.planner.parse;

import org.apache.kylin.KylinSQLException;
import evolution.Commons;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.kylin.sql.planner.calcite.CalciteConfig;
import org.checkerframework.checker.nullness.qual.NonNull;

import static java.util.Objects.requireNonNull;

public class CalciteParser {

    /**
     * Visitor that throws exceptions for unsupported SQL features.
     *
     * <P/>
     * TODO: Implement it.
     */
    private static class  UnsupportedOperationVisitor extends SqlBasicVisitor<Void> {

    }

    private final SqlValidator validator;
    private final RelOptCluster cluster;
    public CalciteParser(SqlValidator validator,
                         @NonNull RelOptCluster cluster) {
        this.validator = validator;
        this.cluster = requireNonNull(cluster, "rowType");
    }

    public SqlNode parse(String sql) {
        SqlNodeList statements = Commons.parse(sql);
        if (statements.size() != 1) {
            throw KylinSQLException.error(KylinSQLException.ErrorCode.PARSING,
                    "The command must contain a single statement");
        }
        SqlNode topNode = statements.get(0);
        SqlNode node = validator.validate(topNode);
        SqlVisitor<Void> visitor = new UnsupportedOperationVisitor();
        node.accept(visitor);
        return node;
    }

    public RelRoot rel(SqlNode node) {
        SqlToRelConverter sqlToRelConverter = createSqlToRelConverter();
        return sqlToRelConverter.convertQuery(node, false, true);
    }

    private SqlToRelConverter createSqlToRelConverter() {

        //TODO: custom StandardConvertletTable
        //TODO: ViewExpander

        return new SqlToRelConverter(
                null,
                validator,
                validator.getCatalogReader().unwrap(CalciteCatalogReader.class),
                cluster,
                StandardConvertletTable.INSTANCE,
                CalciteConfig.DEFAULT_TO_REL_CONVERTER_CONFIG);
    }
}
