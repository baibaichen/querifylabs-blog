package org.apache.kylin.sql.planner.plan.nodes;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

/** Implementation of {@link org.apache.calcite.rel.core.Join} in
 * {@link LogicalSpark logical spark calling convention}. */
public class KylinJoin extends Join implements LogicalSparkRel {

    protected KylinJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    /** Creates an KylinJoin. */
    public static KylinJoin create(
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet =
          cluster.traitSetOf(LogicalSpark.INSTANCE)
            .simplify();
        return new KylinJoin(cluster, traitSet, left, right, condition,
          variablesSet, joinType);
    }

    @Override
    public KylinJoin copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
        return new KylinJoin(getCluster(), traitSet, left, right,
          condition, variablesSet, joinType);
    }
}
