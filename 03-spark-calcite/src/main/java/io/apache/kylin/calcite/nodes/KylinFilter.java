package io.apache.kylin.calcite.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/** Implementation of {@link org.apache.calcite.rel.core.Filter} in
 * {@link LogicalSpark logical spark calling convention}. */
public class KylinFilter extends Filter implements LogicalSparkRel {

    public KylinFilter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode condition) {
        super(cluster, traits, child, condition);

        assert getConvention() == LogicalSpark.INSTANCE;
        assert getConvention() == input.getConvention();
    }

    @Override
    public KylinFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new KylinFilter(getCluster(), traitSet, input, condition);
    }
}
