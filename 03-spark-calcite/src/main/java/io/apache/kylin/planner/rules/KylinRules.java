package io.apache.kylin.planner.rules;

import com.google.common.collect.ImmutableList;
import io.apache.kylin.planner.nodes.KylinConvention;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRule;

import java.util.List;

@Slf4j
public class KylinRules {

    /** Rule that converts a
     *  {@link org.apache.calcite.rel.logical.LogicalTableScan} to
     * {@link KylinConvention enumerable calling convention}. */
    public static final KylinTableScanRule KYLIN_TABLE_SCAN_RULE =
      KylinTableScanRule.DEFAULT_CONFIG
        .toRule(KylinTableScanRule.class);

    public static final List<RelOptRule> KYLIN_RULES = ImmutableList.of(
      KYLIN_TABLE_SCAN_RULE
    );
}
