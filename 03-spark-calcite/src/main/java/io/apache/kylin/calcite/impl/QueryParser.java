package io.apache.kylin.calcite.impl;

import io.apache.kylin.calcite.KylinSQLException;
import io.apache.kylin.calcite.util.Commons;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;

public class QueryParser {

    /**
     * Visitor that throws exceptions for unsupported SQL features.
     *
     * <P/>
     * TODO: Implement it.
     */
    private static class  UnsupportedOperationVisitor extends SqlBasicVisitor<Void> {

    }

    private final SqlValidator validator;

    public QueryParser(SqlValidator validator) {
        this.validator = validator;
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
}
