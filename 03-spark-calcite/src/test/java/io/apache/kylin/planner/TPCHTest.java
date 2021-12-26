package io.apache.kylin.planner;

import io.apache.kylin.planner.nodes.LogicalSpark;
import io.apache.kylin.test.Resource.Util;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class TPCHTest {

    void runSQL(String SQL, Convention convention) throws Exception {
        Optimizer optimizer = Optimizer.of("tpch", Util.TPCH_SCHEMA);
        SqlNode sqlTree = optimizer.parse(SQL);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode convert = optimizer.convert(validatedSqlTree);
        print("AFTER CONVERSION", convert);

        RelTraitSet traitSet = convert.getTraitSet().replace(convention);
        RelNode optimizerRelTree = optimizer.optimize(convert,traitSet, false);
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
        return IntStream.range(0, 22).
                mapToObj(i -> dynamicTest("tpc_" + (i + 1),
                  () -> runSQL(Util.QUERIES.get(i), EnumerableConvention.INSTANCE)));
    }

    @Test
    void testSimple() throws Exception {
        // runSQL("select * from tpch.lineitem where l_shipdate <= date '1998-12-01'", LogicalSpark.INSTANCE); /*  1 */
         runSQL("select l_linestatus from tpch.lineitem where l_shipdate <= date '1998-12-01'",
          LogicalSpark.INSTANCE);                                                                           /*  2 */
    }
}
