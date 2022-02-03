package io.apache.kylin.pp.spark.calcite.table.planner.optimize;

import org.apache.calcite.rel.RelNode;

public class CalciteOptimizer implements Optimizer<RelNode, RelNode> {

    @Override
    public RelNode apply(RelNode relNode) {
        return relNode;
    }
}
