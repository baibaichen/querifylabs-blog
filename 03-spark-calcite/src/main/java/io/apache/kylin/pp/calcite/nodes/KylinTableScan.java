package io.apache.kylin.pp.calcite.nodes;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Util;

import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.TableScan} in
 * {@link LogicalSpark logical spark calling convention}. */
public class KylinTableScan extends TableScan implements LogicalSparkRel {

    protected KylinTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table) {
        super(cluster, traitSet, hints, table);

        assert getConvention() instanceof LogicalSpark;
    }

    /** Returns whether KylinTableScan can generate code to handle a
     * particular variant of the Table SPI.
     **/
    public static boolean canHandle(RelOptTable relOptTable) {
        Util.discard(relOptTable);
        return true;
    }

    /** Creates an KylinTableScan. */
    public static KylinTableScan create(RelOptCluster cluster, RelOptTable relOptTable) {
        final Table table = relOptTable.unwrap(Table.class);
        final RelTraitSet traitSet =
          cluster.traitSetOf(LogicalSpark.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                if (table != null) {
                    return table.getStatistic().getCollations();
                }
                return ImmutableList.of();
            })
            .simplify();
        return new KylinTableScan(cluster, traitSet, ImmutableList.of(), relOptTable);
    }
}
