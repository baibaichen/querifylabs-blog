package io.apache.kylin.spark.calcite.table.planner.delegation;

import io.apache.kylin.calcite.impl.QueryParser;
import io.apache.kylin.table.delegation.Parser;
import io.apache.kylin.table.operations.Operation;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.function.Supplier;

/** Implementation of {@link Parser} that uses Calcite. */
public class ParserImpl implements Parser {
    private final Supplier<QueryParser> calciteParserSupplier;

    public ParserImpl(Supplier<QueryParser> parser) {
        this.calciteParserSupplier = parser;
    }

    @Override
    public List<Operation> parse(String statement) {
        QueryParser parser = calciteParserSupplier.get();

        // parse the sql query
        SqlNode parsed = parser.parse(statement);

        return null;
    }
}
