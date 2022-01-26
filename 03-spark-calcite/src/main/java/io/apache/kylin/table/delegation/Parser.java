package io.apache.kylin.table.delegation;

import io.apache.kylin.calcite.KylinSQLException;
import io.apache.kylin.table.operations.Operation;

import java.util.List;

/** Provides methods for parsing SQL objects from a SQL string. */
public interface Parser {

    /**
     * Entry point for parsing SQL queries expressed as a String.
     *
     * <p><b>Note:</b>If the created {@link Operation} is a {@link QueryOperation} it must be in a
     * form that will be understood by the {@link Planner#translate(List)} method.
     *
     * <p>The produced Operation trees should already be validated.
     *
     * @param statement the SQL statement to evaluate
     * @return parsed queries as trees of relational {@link Operation}s
     * @throws KylinSQLException when failed to parse the statement
     */
    List<Operation> parse(String statement);
}
