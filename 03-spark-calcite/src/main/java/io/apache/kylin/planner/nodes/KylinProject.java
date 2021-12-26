package io.apache.kylin.planner.nodes;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Project} in
 * {@link LogicalSpark logical spark calling convention}. */
public class KylinProject extends Project implements LogicalSparkRel {

    /**
     * Creates an KylinProject.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster  Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param input    Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType  Output row type
     */
    public KylinProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
        super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof LogicalSpark;
    }

    /** Creates an KylinProject, specifying row type rather than field
     * names. */
    public static KylinProject create(
      final RelNode input,
      final List<? extends RexNode> projects,
      RelDataType rowType) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
          cluster.traitSet().replace(LogicalSpark.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> RelMdCollation.project(mq, input, projects));
        return new KylinProject(cluster, traitSet, input, projects, rowType);
    }

    @Override
    public KylinProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new KylinProject(getCluster(), traitSet, input, projects, rowType);
    }
}
