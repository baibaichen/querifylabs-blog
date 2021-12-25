package io.apache.kylin.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class RelOptKylinTable implements RelOptTable  {

    private final @Nullable RelOptSchema schema;
    private final RelDataType rowType;
    private final StarTable table;

    private RelOptKylinTable(
            @Nullable RelOptSchema schema,
            RelDataType rowType,
            StarTable table) {
        this.schema = schema;
        this.rowType = requireNonNull(rowType, "rowType");
        this.table = requireNonNull(table, "table");
    }

    @Override
    public List<String> getQualifiedName() {
        // TODO: getQualifiedName
        return ImmutableList.of("tpch", "kylinstar");
    }

    @Override
    public double getRowCount() {
        return 0;
    }

    @Override
    public RelDataType getRowType() {
        return rowType;
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return null;
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return table.toRel(context, this);
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        return null;
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return null;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return null;
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        return null;
    }

    @Override
    public @Nullable Expression getExpression(Class clazz) {
        return null;
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return null;
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return null;
    }

    @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        if (clazz.isInstance(table)) {
            return clazz.cast(table);
        }
        if (table != null) {
            final T t = ((Wrapper) table).unwrap(clazz);
            if (t != null) {
                return t;
            }
        }
        if (clazz == CalciteSchema.class && schema != null) {
            return clazz.cast(
                    Schemas.subSchema(((CalciteCatalogReader) schema).getRootSchema(),
                            Util.skipLast(getQualifiedName())));
        }
        return null;
    }

    public static RelOptKylinTable of(
      Lattice lattice,
      @Nullable RelOptSchema schema,
      RelDataTypeFactory typeFactory) {
        StarTable starTable = lattice.createStarTable();
        /* TODO: Fix StarTable.getRowType() in case of DerivedColumn
        RelDataType rowType  = starTable.getRowType(typeFactory); */
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
        return new RelOptKylinTable(schema, rowType, starTable);
    }
}
