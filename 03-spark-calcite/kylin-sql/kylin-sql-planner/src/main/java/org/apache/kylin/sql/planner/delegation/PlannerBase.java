package org.apache.kylin.sql.planner.delegation;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.sql.planner.plan.optimize.Optimizer;
import org.apache.kylin.table.delegation.Planner;
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

    protected abstract <T, R> Optimizer<T, R> getOptimizer();
}
