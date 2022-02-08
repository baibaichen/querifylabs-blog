package evolution;

import com.google.common.collect.ImmutableList;
import org.apache.kylin.sql.planner.calcite.RelOptKylinTable;
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

public class LatticeFixer {
    private LatticeFixer() {}

    public static RelOptKylinTable of(
      Lattice lattice,
      @Nullable RelOptSchema schema,
      RelDataTypeFactory typeFactory) {
        StarTable starTable = lattice.createStarTable();

        /* TODO: Fix StarTable.getRowType() in case of DerivedColumn */
        final List<RelDataType> typeList = new ArrayList<>();
        for (Table table : starTable.tables) {
            final RelDataType rowType = table.getRowType(typeFactory);
            typeList.addAll(RelOptUtil.getFieldTypeList(rowType));
        }

        List<String> uniqueColumnNames = lattice.columns.stream()
                .filter(column -> column instanceof Lattice.BaseColumn)
                .map(column -> column.alias)
                .collect(Collectors.toList());
        RelDataType rowType  = typeFactory.createStructType(typeList, uniqueColumnNames);
        return new RelOptKylinTable(schema, rowType, ImmutableList.of("tpch", "kylinstar"), starTable);
    }
}
