package org.apache.kylin.sql.planner.plan.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.kylin.sql.planner.plan.nodes.KylinProject;
import org.apache.kylin.sql.planner.plan.nodes.KylinSort;
import org.apache.kylin.sql.planner.plan.nodes.LogicalSpark;
import org.apache.calcite.plan.RelOptRule;

import java.util.List;

public class KylinRules {
    private KylinRules() {}

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

    /** Rule that converts a
     * {@link org.apache.calcite.rel.logical.LogicalJoin} to
     * {@link LogicalSpark logical spark calling convention}. */
    public static final RelOptRule KYLIN_JOIN_RULE =
      KylinJoinRule.DEFAULT_CONFIG.toRule(KylinJoinRule.class);

    public static final KylinLimitRule KYLIN_LIMIT_RULE =
      KylinLimitRule.Config.DEFAULT.toRule();

    public static final List<RelOptRule> KYLIN_RULES = ImmutableList.of(
      KYLIN_TABLE_SCAN_RULE,
      KYLIN_FILTER_RULE,
      KYLIN_PROJECT_RULE,
      KYLIN_AGGREGATE_RULE,
      KYLIN_SORT_RULE,
      KYLIN_JOIN_RULE,
      KYLIN_LIMIT_RULE
    );


    public static final List<RelOptRule> CANONICALIZE_RULES = ImmutableList.of(
      CoreRules.FILTER_PROJECT_TRANSPOSE,
      CoreRules.FILTER_MERGE,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      CoreRules.PROJECT_MERGE,
      CoreRules.PROJECT_REMOVE,
      CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
      CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,
      CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE
    );

    /** Scan => Filter => Join => Project */
    public static final List<RelOptRule> CANONICALIZE_RULES_PUSH_FILTER = ImmutableList.of(
      CoreRules.FILTER_PROJECT_TRANSPOSE,
      CoreRules.FILTER_MERGE,
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.JOIN_CONDITION_PUSH,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      CoreRules.PROJECT_MERGE,
      CoreRules.PROJECT_REMOVE,
      CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
      CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,
      CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER,
      CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
      CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER,
      CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,
      CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER
    );

    /** Scan => Filter => Project => Join */
    public static final List<RelOptRule> SUBSTITUTE_CANONICALIZE_RULES = ImmutableList.of(
      CoreRules.FILTER_PROJECT_TRANSPOSE,
      CoreRules.FILTER_MERGE,
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.JOIN_CONDITION_PUSH,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      CoreRules.PROJECT_MERGE,
      CoreRules.PROJECT_REMOVE,
      CoreRules.PROJECT_JOIN_TRANSPOSE,
      CoreRules.PROJECT_SET_OP_TRANSPOSE,
      CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS
    );
}
