package org.apache.kylin.sql.planner.delegation;

import org.apache.kylin.sql.planner.parse.CalciteParser;
import org.apache.kylin.sql.planner.operations.PlannerQueryOperation;
import org.apache.kylin.table.delegation.Parser;
import org.apache.kylin.table.operations.Operation;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/** Implementation of {@link Parser} that uses Calcite. */
public class ParserImpl implements Parser {
    private final Supplier<CalciteParser> calciteParserSupplier;

    public ParserImpl(Supplier<CalciteParser> parser) {
        this.calciteParserSupplier = parser;
    }

    @Override
    public List<Operation> parse(String statement) {
        CalciteParser parser = calciteParserSupplier.get();

        // parse the sql query
        SqlNode parsed = parser.parse(statement);
        RelRoot relational =  parser.rel(parsed);
        Operation operation = new PlannerQueryOperation(relational.project());
        return Collections.singletonList(operation);
    }
}
