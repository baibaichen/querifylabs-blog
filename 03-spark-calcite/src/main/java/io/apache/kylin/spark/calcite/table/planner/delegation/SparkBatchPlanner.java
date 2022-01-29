package io.apache.kylin.spark.calcite.table.planner.delegation;

import io.apache.kylin.calcite.impl.QueryParser;
import io.apache.kylin.spark.calcite.PlannerContext;
import io.apache.kylin.table.delegation.Parser;
import org.apache.spark.sql.SparkSession;

public class SparkBatchPlanner extends PlannerBase {

    public SparkBatchPlanner(SparkSession session) {
        super(session);
    }

    @Override
    public Parser getParser() {
        return new ParserImpl(this::createParser);
    }

    private QueryParser createParser() {
        PlannerContext context = getPlannerContext();
        return new QueryParser(context.createSqlValidator(context.createCatalogReader()), context.createCluster());
    }
}
