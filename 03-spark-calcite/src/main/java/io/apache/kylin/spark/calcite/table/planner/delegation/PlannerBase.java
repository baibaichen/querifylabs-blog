package io.apache.kylin.spark.calcite.table.planner.delegation;

import com.google.common.annotations.VisibleForTesting;
import io.apache.kylin.spark.calcite.PlannerContext;
import io.apache.kylin.table.delegation.Planner;
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
}
