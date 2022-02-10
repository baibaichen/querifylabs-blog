package evolution;

import com.google.common.collect.ImmutableList;

import evolution.org.apache.kylin.meta.KylinModelTable;
import evolution.org.apache.kylin.meta.RelOptKylinModelTable;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.StarTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LatticeFactory {
    private LatticeFactory() {}

    public static RelOptKylinModelTable create(Lattice lattice,
                                               @Nullable RelOptSchema schema,
                                               RelDataTypeFactory typeFactory) {
        StarTable starTable = lattice.createStarTable();
        RelDataType rowType = getFixRowType(typeFactory, starTable);
        return new RelOptKylinModelTable(schema, rowType, ImmutableList.of("tpch", "kylinstar"), starTable);
    }
    public static KylinModelTable createKylinModelTable(Lattice lattice) {
        return new KylinModelTable("", null, null, lattice.createStarTable());
    }
    public static RelDataType getFixRowType(RelDataTypeFactory typeFactory, StarTable starTable) {
        RelDataType rowType;
        Lattice lattice = starTable.lattice;
        if (lattice.columns.stream().anyMatch(column -> column instanceof  Lattice.DerivedColumn)) {
            /* TODO: Fix StarTable.getRowType() in case of DerivedColumn */
            final List<RelDataType> typeList = new ArrayList<>();
            for (Table table : starTable.tables) {
                final RelDataType tableRowType = table.getRowType(typeFactory);
                typeList.addAll(RelOptUtil.getFieldTypeList(tableRowType));
            }

            List<String> uniqueColumnNames = lattice.columns.stream()
                    .filter(column -> column instanceof Lattice.BaseColumn)
                    .map(column -> column.alias)
                    .collect(Collectors.toList());
            rowType = typeFactory.createStructType(typeList, uniqueColumnNames);
        } else {
            rowType = starTable.getRowType(typeFactory);
        }
        return rowType;
    }
}
