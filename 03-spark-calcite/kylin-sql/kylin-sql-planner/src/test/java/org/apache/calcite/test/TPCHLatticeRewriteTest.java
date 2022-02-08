package org.apache.calcite.test;

import com.google.common.base.Throwables;
import org.apache.kylin.test.Resource.JdbcTest;
import org.apache.kylin.test.Resource.TPCHSchema;
import org.apache.calcite.test.schemata.foodmart.FoodmartSchema;
import org.apache.calcite.util.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TPCHLatticeRewriteTest {

    /** When a lattice is registered, there is a table with the same name.
     * It can be used for explain, but not for queries. */
    @Test
    void testLatticeStarTable() {
        final AtomicInteger counter = new AtomicInteger();
        try {
            foodmartModel()
                    .query("select count(*) from \"adhoc\".\"star\"")
                    .convertMatches(
                            CalciteAssert.checkRel(""
                                            + "LogicalAggregate(group=[{}], EXPR$0=[COUNT()])\n"
                                            + "  StarTableScan(table=[[adhoc, star]])\n",
                                    counter));
        } catch (Throwable e) {
            assertThat(Throwables.getStackTraceAsString(e),
                    containsString("CannotPlanException"));
        }
        assertThat(counter.get(), equalTo(1));
    }

    private static CalciteAssert.AssertThat foodmartModel(String... extras) {
        final String sql = "select 1\n"
                + "from \"foodmart\".\"sales_fact_1997\" as \"s\"\n"
                + "join \"foodmart\".\"product\" as \"p\" using (\"product_id\")\n"
                + "join \"foodmart\".\"time_by_day\" as \"t\" using (\"time_id\")\n"
                + "join \"foodmart\".\"product_class\" as \"pc\"\n"
                + "  on \"p\".\"product_class_id\" = \"pc\".\"product_class_id\"";
        return modelWithLattice(TPCHLatticeRewriteTest::modelWithLattices, "star", sql, extras);
    }
    private static CalciteAssert.AssertThat modelWithLattices(
            String... lattices) {
        final Class<JdbcTest.EmpDeptTableFactory> clazz =
                JdbcTest.EmpDeptTableFactory.class;
        return CalciteAssert.model(""
                + "{\n"
                + "  version: '1.0',\n"
                + "   schemas: [\n"
                + FoodmartSchema.FOODMART_SCHEMA
                + ",\n"
                + "     {\n"
                + "       name: 'adhoc',\n"
                + "       tables: [\n"
                + "         {\n"
                + "           name: 'EMPLOYEES',\n"
                + "           type: 'custom',\n"
                + "           factory: '"
                + clazz.getName()
                + "',\n"
                + "           operand: {'foo': true, 'bar': 345}\n"
                + "         }\n"
                + "       ],\n"
                + "       lattices: "
                + Arrays.toString(lattices)
                + "     }\n"
                + "   ]\n"
                + "}").withDefaultSchema("adhoc");
    }

    private static CalciteAssert.AssertThat modelWithLattice(
      Function<String, CalciteAssert.AssertThat> modelWithLattices,
      String name, String sql, String... extras) {
        final StringBuilder buf = new StringBuilder("{ name: '")
                .append(name)
                .append("', sql: ")
                .append(TestUtil.escapeString(sql));
        for (String extra : extras) {
            buf.append(", ").append(extra);
        }
        buf.append("}");
        return modelWithLattices.apply(buf.toString());
    }

    @Test
    void testLatticeStarTable2() {
            tpchModel()
                    .query("select count(*) from \"adhoc\".\"star\"")
                    .enableMaterializations(true)
                    .explainContains("EnumerableTableScan(table=[[adhoc, m{}]])");
    }
    private static CalciteAssert.AssertThat tpchModel(){
        final String extra =
                "auto: true,\n" +
                "algorithm: true,\n" +
                "defaultMeasures: [ {agg: 'count'} ]";
        final String sql =
                "select 1 from TPCH_01.customer c \n" +
                "  inner join TPCH_01.orders o   on c.c_custkey = o.o_custkey\n" +
                "  inner join TPCH_01.lineitem l on o.o_orderkey = l.l_orderkey\n" +
                "  inner join TPCH_01.supplier s on l.l_suppkey = s.s_suppkey\n" +
                "  inner join TPCH_01.nation n   on  s.s_nationkey = n.n_nationkey\n" +
                "  inner join TPCH_01.region r   on  n.n_regionkey = r.r_regionkey";
        return modelWithLattice(TPCHLatticeRewriteTest::withLattices, "star", sql, extra);
    }
    private static CalciteAssert.AssertThat withLattices(
      String... lattices) {
        String model = "{\n" +
                "  \"version\": \"1.0\",  \n" +
                "  \"schemas\": [\n" +
                TPCHSchema.TPCH_01_SCHEMA + ",\n" +
                "     {\n" +
                "        name: 'adhoc',\n" +
                "        lattices: \n" + Arrays.toString(lattices) +
                "     } \n" +
                "   ]\n" +
                "}";
        return CalciteAssert.model(model).withDefaultSchema(TPCHSchema.TPCH_01_NAME);
    }
}
