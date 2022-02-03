package io.apache.kylin.pp.spark.calcite.table.planner.delegation;

import com.google.common.annotations.VisibleForTesting;
import io.apache.kylin.pp.spark.calcite.PlannerContext;
import io.apache.kylin.api.table.delegation.Planner;
import io.apache.kylin.pp.spark.calcite.table.planner.optimize.Optimizer;
import org.apache.spark.sql.SparkSession;

abstract class PlannerBase implements Planner {

    private final PlannerContext plannerContext;

    protected PlannerBase(SparkSession session) {
        plannerContext = new PlannerContext(session);
    }

    @VisibleForTesting
    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    protected abstract <T,R> Optimizer<T, R> getOptimizer();
}
