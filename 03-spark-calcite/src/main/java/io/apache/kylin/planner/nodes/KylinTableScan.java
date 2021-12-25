package io.apache.kylin.planner.nodes;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Table;

import java.util.List;

public class KylinTableScan extends TableScan implements KylinRel {

    protected KylinTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    /** Returns whether KylinTableScan can generate code to handle a
     * particular variant of the Table SPI.
     **/
    public static boolean canHandle(RelOptTable relOptTable) {
        return true;
    }

    /** Creates an KylinTableScan. */
    public static KylinTableScan create(RelOptCluster cluster, RelOptTable relOptTable) {
        final Table table = relOptTable.unwrap(Table.class);
        final RelTraitSet traitSet =
          cluster.traitSetOf(KylinConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                if (table != null) {
                    return table.getStatistic().getCollations();
                }
                return ImmutableList.of();
            });
        return new KylinTableScan(cluster, traitSet, ImmutableList.of(), relOptTable);
    }
}
