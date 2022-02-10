package org.apache.calcite.materialize;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.plan.RelOptTable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum TPCHStatisticProvider implements SqlStatisticProvider {
    INSTANCE;

    private final ImmutableMap<String, Double> cardinalityMap;
    private final ImmutableMultimap<String, ImmutableList<String>> keyMap;
    private final ImmutableMultimap<String, ImmutableList<String>> FkeyMap;
    TPCHStatisticProvider() {
        final Initializer initializer = new Initializer()
                .put("tpch", "PART", 0, "P_PARTKEY")
                .put("tpch", "SUPPLIER", 0, "S_SUPPKEY")
                .put("tpch", "PARTSUPP", 0, "PS_PARTKEY", "PS_SUPPKEY")
                .put("tpch", "CUSTOMER", 0, "C_CUSTKEY")
                .put("tpch", "ORDERS", 0, "O_ORDERKEY")
                .put("tpch", "LINEITEM", 0, "L_ORDERKEY", "L_LINENUMBER")
                .put("tpch", "NATION", 0, "N_NATIONKEY")
                .put("tpch", "REGION", 0, "R_REGIONKEY")
                .putFK("tpch", "SUPPLIER", "S_NATIONKEY")
                .putFK("tpch", "PARTSUPP", "PS_SUPPKEY")
                .putFK("tpch", "PARTSUPP", "PS_PARTKEY")
                .putFK("tpch", "CUSTOMER", "C_NATIONKEY")
                .putFK("tpch", "ORDERS", "O_CUSTKEY")
                .putFK("tpch", "LINEITEM", "L_ORDERKEY")
                .putFK("tpch", "LINEITEM", "L_PARTKEY")
                .putFK("tpch", "LINEITEM", "L_SUPPKEY")
                .putFK("tpch", "LINEITEM", "L_PARTKEY", "L_SUPPKEY")
                .putFK("tpch", "NATION", "N_REGIONKEY");

        cardinalityMap = initializer.cardinalityMapBuilder.build();
        keyMap = initializer.keyMapBuilder.build();
        FkeyMap = initializer.FkeyMapBuilder.build();
    }

    @Override
    public double tableCardinality(RelOptTable table) {
        final List<String> qualifiedName =
                table.maybeUnwrap(JdbcTable.class)
                        .map(value ->
                                Arrays.asList(value.jdbcSchemaName, value.jdbcTableName))
                        .orElseGet(table::getQualifiedName);
        return cardinalityMap.get(qualifiedName.toString());
    }

    @Override
    public boolean isForeignKey(RelOptTable fromTable, List<Integer> fromColumns,
                                RelOptTable toTable, List<Integer> toColumns) {
        boolean result = fromColumns.stream().allMatch(columnOrdinal -> (columnOrdinal >= 0)
                && (columnOrdinal < fromTable.getRowType().getFieldCount()));
        assert result;
        ImmutableCollection<ImmutableList<String>> x =  FkeyMap.get(fromTable.getQualifiedName().toString());
        List<String> y = columnNames(fromTable, fromColumns);
        return  x.contains(y);
    }

    @Override
    public boolean isKey(RelOptTable table, List<Integer> columns) {
        // In order to match, all column ordinals must be in range 0 .. columnCount
        return columns.stream().allMatch(columnOrdinal ->
                (columnOrdinal >= 0)
                        && (columnOrdinal < table.getRowType().getFieldCount()))
                // ... and the column names match the name of the primary key
                && keyMap.get(table.getQualifiedName().toString()).contains(columnNames(table, columns));
    }
    private static List<String> columnNames(RelOptTable table, List<Integer> columns) {
        return columns.stream()
                .map(columnOrdinal -> table.getRowType().getFieldNames()
                        .get(columnOrdinal))
                .collect(Collectors.toList());
    }

    /** Helper during construction. */
    private static class Initializer {
        final ImmutableMap.Builder<String, Double> cardinalityMapBuilder =
                ImmutableMap.builder();
        final ImmutableMultimap.Builder<String, ImmutableList<String>> keyMapBuilder =
                ImmutableMultimap.builder();

        final ImmutableMultimap.Builder<String, ImmutableList<String>> FkeyMapBuilder =
                ImmutableMultimap.builder();

        Initializer put(String schema, String table, int count, String... keys) {
            String qualifiedName = Arrays.asList(schema, table).toString();
            cardinalityMapBuilder.put(qualifiedName, (double) count);
            return put(schema, table, keys);
        }
        
        Initializer put(String schema, String table, String... keys) {
            String qualifiedName = Arrays.asList(schema, table).toString();
            final ImmutableList<String> keyList = ImmutableList.copyOf((String[]) keys);
            keyMapBuilder.put(qualifiedName, keyList);
            return this;
        }

        Initializer putFK(String schema, String table, String... keys) {
            String qualifiedName = Arrays.asList(schema, table).toString();
            final ImmutableList<String> keyList = ImmutableList.copyOf(keys); // composite key
            FkeyMapBuilder.put(qualifiedName, keyList);
            return this;
        }
    }
}
