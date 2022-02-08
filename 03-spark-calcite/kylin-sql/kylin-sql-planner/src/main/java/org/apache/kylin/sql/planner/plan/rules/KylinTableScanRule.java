package org.apache.kylin.sql.planner.plan.rules;


import org.apache.kylin.sql.planner.plan.nodes.KylinTableScan;
import org.apache.kylin.sql.planner.plan.nodes.LogicalSpark;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Planner rule that converts a {@link LogicalTableScan} to an {@link KylinTableScan}.
 * You may provide a custom config to convert other nodes that extend {@link TableScan}.
 *
 */
public class KylinTableScanRule extends ConverterRule {

    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalTableScan.class,
        r -> KylinTableScan.canHandle(r.getTable()),
        Convention.NONE, LogicalSpark.INSTANCE, "KylinTableScanRule")
      .withRuleFactory(KylinTableScanRule::new);

    protected KylinTableScanRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        TableScan scan = (TableScan) rel;
        final RelOptTable relOptTable = scan.getTable();
        /* final Table table = relOptTable.unwrap(Table.class) */
        return KylinTableScan.create(scan.getCluster(), relOptTable);
    }
}
