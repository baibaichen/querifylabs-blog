package org.apache.kylin.sql.planner.plan.rules;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.List;

/**
 * Planner rule that matches a
 * {@link LogicalJoin} one of whose inputs is a
 * {@link LogicalFilter}s, and
 * pulls the filter up.
 *
 */
@Value.Enclosing
@Slf4j
public class JoinFilterTransposeRule
        extends RelRule<JoinFilterTransposeRule.Config>
        implements TransformationRule {

    /** Creates a JoinPullUpFilterRule.*/
    protected JoinFilterTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        LogicalFilter filter = call.rel(1);
        RelNode newLeft = filter.getInput();

        final List<RexNode> leftFilters = RelOptUtil.conjunctions(filter.getCondition());
        RelNode newJoin = join.copy(join.getTraitSet(), Lists.newArrayList(newLeft, join.getRight()));
        RelBuilder builder = call.builder();
        RelNode finalFilter = builder.push(newJoin).filter(leftFilters).build();
        call.transformTo(finalFilter);
    }

    /**
     * Rule configuration.
     */
    @Value.Immutable(singleton=true)
    public interface Config extends RelRule.Config {
        Config LEFT = ImmutableJoinFilterTransposeRule.Config.of()
                .withOperandSupplier(b0 -> b0.operand(LogicalJoin.class)
                        .predicate(join -> join.getJoinType() == JoinRelType.INNER)
                        .inputs(b1 -> b1.operand(LogicalFilter.class).anyInputs()))
                .withDescription("JoinPullUpFilterRule(Filter-Other)");

        @Override default JoinFilterTransposeRule toRule() {
            return new JoinFilterTransposeRule(this);
        }
    }
}
