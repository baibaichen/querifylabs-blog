package io.apache.kylin.planner.rules;

import com.google.common.collect.ImmutableList;
import io.apache.kylin.planner.nodes.KylinProject;
import io.apache.kylin.planner.nodes.KylinSort;
import io.apache.kylin.planner.nodes.LogicalSpark;
import lombok.extern.slf4j.Slf4j;

import org.apache.calcite.plan.RelOptRule;

import java.util.List;

@Slf4j
public class KylinRules {

    /** Rule that converts a
     *  {@link org.apache.calcite.rel.logical.LogicalTableScan} to
     * {@link LogicalSpark logical spark calling convention}. */
    public static final KylinTableScanRule KYLIN_TABLE_SCAN_RULE =
      KylinTableScanRule.DEFAULT_CONFIG.toRule(KylinTableScanRule.class);

    public static final KylinFilterRule KYLIN_FILTER_RULE =
      KylinFilterRule.DEFAULT_CONFIG.toRule(KylinFilterRule.class);

    /** Rule that converts a
     *  {@link org.apache.calcite.rel.logical.LogicalProject} to an
     * {@link KylinProject}. */
    public static final KylinProjectRule KYLIN_PROJECT_RULE =
      KylinProjectRule.DEFAULT_CONFIG.toRule(KylinProjectRule.class);

    public static final KylinAggregateRule KYLIN_AGGREGATE_RULE =
      KylinAggregateRule.DEFAULT_CONFIG.toRule(KylinAggregateRule.class);

    /** Rule that converts a {@link org.apache.calcite.rel.core.Sort} to an
     * {@link KylinSort}. */
    public static final KylinSortRule KYLIN_SORT_RULE =
      KylinSortRule.DEFAULT_CONFIG.toRule(KylinSortRule.class);

    public static final List<RelOptRule> KYLIN_RULES = ImmutableList.of(
      KYLIN_TABLE_SCAN_RULE,
      KYLIN_FILTER_RULE,
      KYLIN_PROJECT_RULE,
      KYLIN_AGGREGATE_RULE,
      KYLIN_SORT_RULE
    );
}
