package io.apache.kylin.pp.calcite.rules;

import io.apache.kylin.pp.calcite.nodes.LogicalSpark;

import io.apache.kylin.pp.calcite.nodes.KylinFilter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert a {@link LogicalFilter} to an {@link KylinFilter}.
 * You may provide a custom config to convert other nodes that extend {@link Filter}.
 *
 * @see KylinRules#KYLIN_FILTER_RULE
 */
public class KylinFilterRule extends ConverterRule {

    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalFilter.class, Convention.NONE,
        LogicalSpark.INSTANCE, "KylinFilterRule")
      .withRuleFactory(KylinFilterRule::new);

    protected KylinFilterRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final Filter filter = (Filter) rel;
        return new KylinFilter(rel.getCluster(),
          rel.getTraitSet().replace(LogicalSpark.INSTANCE),
          convert(filter.getInput(),
            filter.getInput().getTraitSet()
              .replace(LogicalSpark.INSTANCE)),
          filter.getCondition());
    }
}
