package org.apache.kylin.sql.planner.delegation;

import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.kylin.sql.planner.calcite.SparkTypeFactory;
import org.apache.kylin.sql.planner.catalog.SparkSessionCalciteSchema;
import org.apache.kylin.sql.planner.plan.optimize.Optimizer;
import org.apache.kylin.table.delegation.Planner;
import org.apache.spark.sql.SparkSession;

abstract class PlannerBase implements Planner {

    protected final PlannerContext plannerContext;

    protected PlannerBase(SparkSession session) {
        final CalciteSchema schema = CalciteSchemaBuilder.asRootSchema(new SparkSessionCalciteSchema(session));
        plannerContext = new PlannerContext(schema, new SparkTypeFactory());
    }

    @VisibleForTesting
    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    protected abstract <T, R> Optimizer<T, R> getOptimizer();
}
