package io.apache.kylin.pp.calcite.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Implementation of {@link org.apache.calcite.rel.core.Sort} in
 * {@link LogicalSpark logical spark calling convention}. */
public class KylinSort extends Sort implements LogicalSparkRel{

    /**
     * Creates an KylinSort.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    protected KylinSort(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);

        assert getConvention() instanceof LogicalSpark;
        assert getConvention() == input.getConvention();
        assert fetch == null : "fetch must be null";
        assert offset == null : "offset must be null";
    }

    /** Creates an KylinSort. */
    public static KylinSort create(
      RelNode child,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
        final RelOptCluster cluster = child.getCluster();
        final RelTraitSet traitSet =
          cluster.traitSetOf(LogicalSpark.INSTANCE)
            .replace(collation)
            .simplify();
        return new KylinSort(cluster, traitSet, child, collation, offset, fetch);
    }

    @Override
    public KylinSort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
        return new KylinSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }
}
