package org.apache.kylin.sql.planner.plan.optimize;

import org.apache.calcite.rel.RelNode;

public class CalciteOptimizer implements Optimizer<RelNode, RelNode> {

    @Override
    public RelNode apply(RelNode relNode) {
        return relNode;
    }
}
