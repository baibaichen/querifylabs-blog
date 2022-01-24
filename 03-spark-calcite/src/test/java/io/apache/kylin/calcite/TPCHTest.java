package io.apache.kylin.calcite;

import com.google.common.collect.ImmutableList;
import evolution.io.apache.kylin.calcite.Optimizer;
import io.apache.kylin.calcite.nodes.LogicalSpark;
import io.apache.kylin.test.Resource.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@Slf4j
class TPCHTest {

    void runSQL(String SQL, Convention convention) throws Exception {
        Optimizer optimizer;
        if (convention == EnumerableConvention.INSTANCE) {
            optimizer = Optimizer.of("tpch", Util.TPCH_SCHEMA);
        } else {
            optimizer = Optimizer.of("tpch", Util.TPCH_SCHEMA, ImmutableList.of(), true);
        }
        SqlNode sqlTree = optimizer.parse(SQL);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode convert = optimizer.convert(validatedSqlTree);
        print("AFTER CONVERSION", convert);

        RelTraitSet traitSet = convert.getTraitSet().replace(convention);
        RelNode optimizerRelTree = optimizer.optimize(convert, traitSet, true);
        print("AFTER OPTIMIZATION", optimizerRelTree);
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter =
                new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw);
    }

    @TestFactory
    Stream<DynamicTest> tpchTest() {
        List<Convention> conventions=
          ImmutableList.of(LogicalSpark.INSTANCE, EnumerableConvention.INSTANCE);

        return conventions.stream()
          .flatMap(convention -> IntStream.range(0, 22)
            .mapToObj(i ->
              dynamicTest(convention.getName() + " : tpc_" + (i + 1),
                () -> runSQL(Util.QUERIES.get(i), convention))));
    }

    static final String SCAN_0 = "select * from tpch.lineitem";
    static final String FILTER_1 = "select * from tpch.lineitem where l_shipdate <= date '1998-12-01'";
    static final String PROJECT_2 = "select l_linestatus from tpch.lineitem where l_shipdate <= date '1998-12-01'";
    static final String AGGREGATE_3 = "select min(l_linestatus) from tpch.lineitem where l_shipdate <= date '1998-12-01'";
    static final String SORT_4 = "select min(l_linestatus) from tpch.lineitem where l_shipdate <= date '1998-12-01' order by 1";
    static final String LIMIT_5 = "select * from tpch.lineitem where l_shipdate <= date '1998-12-01' limit 100";

    static final List<String> SimpleSQLs = ImmutableList.of(
      SCAN_0,
      FILTER_1,
      PROJECT_2,
      AGGREGATE_3,
      SORT_4,
      LIMIT_5
    );

    @TestFactory
    Stream<DynamicTest> testSimple() {
        return SimpleSQLs.stream().map(sql -> dynamicTest(sql, () -> runSQL(sql, LogicalSpark.INSTANCE)));
    }
}
