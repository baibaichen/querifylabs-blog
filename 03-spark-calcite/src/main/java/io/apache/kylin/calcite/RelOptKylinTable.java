package io.apache.kylin.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RelOptKylinTable implements RelOptTable  {

    private final @Nullable RelOptSchema schema;
    private final RelDataType            rowType;
    private final List<String>           qualifiedTblName;
    private final Table                  table;

    public RelOptKylinTable(
            @Nullable RelOptSchema schema,
            RelDataType rowType,
            List<String> qualifiedTblName,
            Table table) {
        this.schema = schema;
        this.rowType = requireNonNull(rowType, "rowType");
        this.qualifiedTblName = ImmutableList.copyOf(qualifiedTblName);
        this.table = requireNonNull(table, "table");
    }

    @Override
    public List<String> getQualifiedName() {
        return qualifiedTblName;
    }

    @Override
    public double getRowCount() {
        // TODO: getRowCount()
        return 0;
    }

    @Override
    public RelDataType getRowType() {
        return rowType;
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return schema;
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        if (table instanceof TranslatableTable) {
            return ((TranslatableTable) table).toRel(context, this);
        }
        // TODO: return KylinTableScan?
        return LogicalTableScan.create(context.getCluster(), this, context.getTableHints());
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        //TODO: getCollationList()
        return ImmutableList.of();
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        // TODO: isKey
        return false;
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Expression getExpression(Class clazz) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        throw new UnsupportedOperationException();
    }

    @Override public <T> @Nullable T unwrap(Class<T> clazz) {
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
}
