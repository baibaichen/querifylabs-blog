package io.apache.kylin.calcite.planner.planner.nodes.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class KylinTableScan extends TableScan implements KylinLogicalNode {

    protected KylinTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }
}
