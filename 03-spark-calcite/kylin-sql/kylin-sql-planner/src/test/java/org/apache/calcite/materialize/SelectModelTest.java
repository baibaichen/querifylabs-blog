package org.apache.calcite.materialize;

import evolution.Debugger;
import evolution.LatticeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.sql.planner.delegation.PlannerContext;
import org.apache.kylin.test.Resource.LatticeHEP;
import org.apache.kylin.test.Resource.TPCH;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.materialize.ModelBuilder.LINEITEM_PART_MODEL;

@Slf4j
public class SelectModelTest {

    static final String MODEL_SQL =
            "select * from TPCH_01.customer c \n" +
                    "  inner join TPCH_01.orders o   on c.c_custkey = o.o_custkey\n" +
                    "  inner join TPCH_01.lineitem l on o.o_orderkey = l.l_orderkey\n" +
                    "  inner join TPCH_01.supplier s on l.l_suppkey = s.s_suppkey\n" +
                    "  inner join TPCH_01.nation n   on  s.s_nationkey = n.n_nationkey\n" +
                    "  inner join TPCH_01.region r   on  n.n_regionkey = r.r_regionkey";

    @Test
    void testSelectModel() {
        CalciteSchema calciteSchema = CalciteSchemaBuilder.createRootSchemaWithChild(TPCH.SCHEMA, TPCH.NAME_01);
        Lattice.Builder latticeBuilder = Lattice.builder(new LatticeSpace(TPCHStatisticProvider.INSTANCE) ,calciteSchema, MODEL_SQL);
        Lattice lattice = latticeBuilder.build();
        PlannerContext plannerContext = new PlannerContext(calciteSchema, new JavaTypeFactoryImpl());
        RelOptLattice relOptLattice =
                new RelOptLattice(lattice, LatticeFactory.create(lattice, plannerContext.createCatalogReader(), plannerContext.getTypeFactory()));
        Assertions.assertNotNull(lattice);
        final String aggSQL =
                "select o.o_orderdate, count(*) from TPCH_01.orders o \n" +
                        "  inner join TPCH_01.customer c on o.o_custkey = c.c_custkey\n" +
                        "  inner join TPCH_01.lineitem l on o.o_orderkey = l.l_orderkey\n" +
                        "  inner join TPCH_01.supplier s on l.l_suppkey = s.s_suppkey\n" +
                        "  inner join TPCH_01.nation n   on  s.s_nationkey = n.n_nationkey\n" +
                        "  inner join TPCH_01.region r   on  n.n_regionkey = r.r_regionkey\n" +
                        "where l.l_orderkey >= s.s_suppkey group by o.o_orderdate";

        final String simpleSQL =
                "select * from TPCH_01.orders o \n" +
                        "  inner join TPCH_01.customer c on o.o_custkey = c.c_custkey\n" +
                        "  inner join TPCH_01.lineitem l on o.o_orderkey = l.l_orderkey\n" +
                        "  inner join TPCH_01.supplier s on l.l_suppkey = s.s_suppkey\n" +
                        "  inner join TPCH_01.nation n   on  s.s_nationkey = n.n_nationkey\n" +
                        "  inner join TPCH_01.region r   on  n.n_regionkey = r.r_regionkey\n" +
                        "where l.l_orderkey >= o.o_orderkey";

        RelNode rel = plannerContext.createParser().rel(simpleSQL).rel;
        RelNode rewriteRel = relOptLattice.rewrite(LatticeHEP.toLeafJoinForm(rel));
        Assertions.assertNotNull(rewriteRel);
        log.info("Rewrite plan :\n {}", Debugger.toString(rewriteRel));
        log.info(" Rewrite SQL :\n {}", Debugger.toSparkSql(rewriteRel));
        RelNode leafAgain2 = LatticeHEP.toLeafJoinForm(rewriteRel);
        log.info("Rewrite to Leaf plan :\n {}", Debugger.toString(leafAgain2));
        log.info(" Rewrite to Leaf SQL :\n {}", Debugger.toSparkSql(leafAgain2));
    }

    @Test
    void testAddModelIntoSchema() {
        PlannerContext plannerContext = new ModelBuilder.ContextBuilder()
          .setSchemaName("adhoc")
          .setModelSQL("base_view_mv", LINEITEM_PART_MODEL)
          .buildPlannerContext();

        RelNode rel = plannerContext.createParser().rel("select count(*) from adhoc.base_view_mv group by p_type").rel;
        Assertions.assertNotNull(rel);
        log.info("CANONICALIZE PLAN :\n {}", Debugger.toString((LatticeHEP.toLeafJoinForm(rel))));
    }
}
