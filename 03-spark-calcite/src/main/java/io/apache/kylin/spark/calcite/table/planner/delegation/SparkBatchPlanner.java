package io.apache.kylin.spark.calcite.table.planner.delegation;

import io.apache.kylin.calcite.impl.QueryParser;
import io.apache.kylin.table.delegation.Parser;
import org.apache.spark.sql.SparkSession;

public class SparkBatchPlanner extends PlannerBase {

    public SparkBatchPlanner(SparkSession session) {
        super(session);
    }

    @Override
    public Parser getParser() {
        return new ParserImpl(
                () -> new QueryParser(getPlannerContext().createSqlValidator(getPlannerContext().createCatalogReader()))
        );
    }
}
