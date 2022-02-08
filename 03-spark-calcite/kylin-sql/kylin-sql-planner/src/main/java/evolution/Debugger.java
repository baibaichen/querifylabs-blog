package evolution;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;

import java.util.function.UnaryOperator;

public class Debugger {
    private Debugger() {}

    public static String toString(RelNode rel) {
        return org.apache.calcite.util.Util.toLinux(
                RelOptUtil.dumpPlan("", rel, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }

    /** Converts a relational expression to SQL. */
    public static String toSql(RelNode root) {
        return toSql(root, SqlDialect.DatabaseProduct.SPARK.getDialect());
    }

    /** Converts a relational expression to SQL in a given dialect. */
    public static String toSql(RelNode root, SqlDialect dialect) {
        return toSql(root, dialect, c ->
                c.withAlwaysUseParentheses(false)
                        .withSelectListItemsOnSeparateLines(false)
                        .withUpdateSetListNewline(false)
                        .withIndentation(0));
    }

    /** Converts a relational expression to SQL in a given dialect
     * and with a particular writer configuration. */
    public static String toSql(RelNode root, SqlDialect dialect,
                                UnaryOperator<SqlWriterConfig> transform) {
        final RelToSqlConverter converter = new RelToSqlConverter(dialect);
        final SqlNode sqlNode = converter.visitRoot(root).asStatement();
        return sqlNode.toSqlString(c -> transform.apply(c.withDialect(dialect)))
                .getSql();
    }
}
