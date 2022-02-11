/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.calcite.materialize;

import com.google.common.collect.ImmutableList;
import evolution.Debugger;
import evolution.org.apache.kylin.meta.KylinMaterializedViewsRegistry;
import evolution.org.apache.kylin.meta.KylinRelOptMaterialization;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.kylin.sql.planner.delegation.PlannerContext;
import org.apache.kylin.sql.planner.plan.nodes.KylinTableScan;
import org.apache.kylin.sql.planner.plan.nodes.LogicalSpark;
import org.apache.kylin.test.RelNodeAssert;
import org.apache.kylin.test.Resource.TPCH;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Programs;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assertions;
import static org.apache.calcite.materialize.ModelBuilder.LINEITEM_PART_MODEL;
import static org.apache.kylin.sql.planner.plan.rules.KylinRules.KYLIN_RULES;

@Slf4j
class KylinMVTest {
    private static final String VIEW_SQL = TPCH.sql(1);

    @Disabled("need to investigate")
    @Test
    void test_sql1() {
        final String sql = TPCH.sql(1);
        KylinMaterializedViewsRegistry viewRegistry = new KylinMaterializedViewsRegistry();
        PlannerContext plannerContext = TPCH.newPlannerContext();
        KylinRelOptMaterialization x = viewRegistry.createMaterialization(plannerContext, VIEW_SQL, "x");
        Assertions.assertThat(x.tableRel).isInstanceOf(KylinTableScan.class);
        RelNode query = plannerContext.createParser().rel(sql).rel;
        RelNode rewrite = optimize2(query, x);
        Assertions.assertThat(rewrite).isNotNull();
        RelNodeAssert.assertThat(rewrite).contains("KylinTableScan(table=[[x, y]])");
    }

    private RelNode optimize2(RelNode queryRel, KylinRelOptMaterialization materialization) {
        RelOptPlanner planner = queryRel.getCluster().getPlanner();
        RelTraitSet traitSet = queryRel.getCluster().traitSet().replace(LogicalSpark.INSTANCE);
        RegisterRules.registerDefaultRules(planner, true, false, false);
        KYLIN_RULES.forEach(planner::addRule);
        RelNode rewrite = Programs.standard()
                .run(planner, queryRel, traitSet, ImmutableList.of(materialization), ImmutableList.of());
        log.info("Rewrite Plan: \n{}", Debugger.toString(rewrite));
        log.info(" Rewrite SQL: \n{}\n", Debugger.toSparkSql(rewrite));
        return rewrite;
    }

    private KylinRelOptMaterialization createMaterialzation(PlannerContext plannerContext, String mv_sql) {
        KylinMaterializedViewsRegistry viewRegistry = new KylinMaterializedViewsRegistry();
        KylinRelOptMaterialization x = viewRegistry.createMaterialization(plannerContext, mv_sql, "x");
        Assertions.assertThat(x.tableRel).isInstanceOf(KylinTableScan.class);
        log.info("mv plan :\n {}", Debugger.toString(x.queryRel));
        log.info(" mv sql :\n {}", Debugger.toSparkSql(x.queryRel));
        return x;
    }

    @Test
    void testAggregateRollup() {
        PlannerContext plannerContext = new ModelBuilder.ContextBuilder()
          .setSchemaName("adhoc")
          .setModelSQL("base_view_mv", LINEITEM_PART_MODEL)
          .buildPlannerContext();

        // 1.
        final String mv_sql = "select L_SHIPDATE, P_PARTKEY, P_NAME, count(*) as cnt, sum(L_QUANTITY) as quantity_sum from adhoc.base_view_mv group by L_SHIPDATE, P_PARTKEY, P_NAME";
        KylinRelOptMaterialization x = createMaterialzation(plannerContext, mv_sql);
        final String sql1 = "select P_NAME, count(*), sum(L_QUANTITY) from adhoc.base_view_mv group by P_NAME";
        RelNode query = plannerContext.createParser().rel(sql1).rel;
        RelNodeAssert
          .assertThat(optimize2(query, x))
          .contains(
            "KylinAggregate(group=[{2}], EXPR$1=[$SUM0($3)], EXPR$2=[SUM($4)])\n" +
            "  KylinTableScan(table=[[x, y]])");
        plannerContext.getPlanner().clear();
        final String sql2 = "select PS_COMMENT, P_NAME, sum(L_QUANTITY) from tpch.PARTSUPP inner join adhoc.base_view_mv on PS_PARTKEY = P_PARTKEY group by PS_COMMENT,P_NAME";
        RelNodeAssert
          .assertThat(optimize2(plannerContext.createParser().rel(sql2).rel, x))
          .contains(
            "KylinAggregate(group=[{4, 7}], EXPR$2=[SUM($9)])\n" +
            "  KylinJoin(condition=[=($0, $6)], joinType=[inner])\n" +
            "    KylinTableScan(table=[[tpch, PARTSUPP]])\n" +
            "    KylinTableScan(table=[[x, y]])");
        plannerContext.getPlanner().clear();

        //2.
        String mv_with_express =
          "select extract(year from l_shipdate) as thisYear, P_PARTKEY, P_NAME, count(*) as cnt, sum(L_QUANTITY) as quantity_sum from adhoc.base_view_mv group by extract(year from l_shipdate), P_PARTKEY, P_NAME";
        KylinRelOptMaterialization y = createMaterialzation(plannerContext, mv_with_express);
        final String sql3 = "select extract(year from l_shipdate), count(*), sum(L_QUANTITY) from adhoc.base_view_mv where extract(year from l_shipdate) > 2000  group by extract(year from l_shipdate)";
        final String sql4 = "select xxx, count(*), sum(L_QUANTITY) from  (select extract(year from l_shipdate) xxx, L_QUANTITY from adhoc.base_view_mv ) t where t.xxx > 2000  group by t.xxx";
        RelNodeAssert
          .assertThat(optimize2(plannerContext.createParser().rel(sql4).rel, y))
          .contains(
            "KylinAggregate(group=[{0}], EXPR$1=[$SUM0($3)], EXPR$2=[SUM($4)])\n" +
            "  KylinFilter(condition=[<(2000, $0)])\n" +
            "    KylinTableScan(table=[[x, y]])");
    }

    @Disabled("need to fix")
    @Test
    void testDifferentPlanner() {
        CalciteSchema root = new ModelBuilder.ContextBuilder()
          .setSchemaName("adhoc")
          .setModelSQL("base_view_mv", LINEITEM_PART_MODEL)
          .buildRootSchema();
        final String mv_sql = "select L_SHIPDATE, P_PARTKEY, P_NAME, count(*) as cnt, sum(L_QUANTITY) as quantity_sum from adhoc.base_view_mv group by L_SHIPDATE, P_PARTKEY, P_NAME";

        PlannerContext plannerContext = new PlannerContext(root, new JavaTypeFactoryImpl());
        KylinRelOptMaterialization x = createMaterialzation(plannerContext, mv_sql);
        final String sql1 = "select P_NAME, count(*), sum(L_QUANTITY) from adhoc.base_view_mv group by P_NAME";
        RelNode query = plannerContext.createParser().rel(sql1).rel;
        optimize2(query, x);

        PlannerContext plannerContext2 = new PlannerContext(root, new JavaTypeFactoryImpl());
        final String sql2 = "select PS_COMMENT, P_NAME, sum(L_QUANTITY) from tpch.PARTSUPP inner join adhoc.base_view_mv on PS_PARTKEY = P_PARTKEY group by PS_COMMENT,P_NAME";
        optimize2(plannerContext2.createParser().rel(sql2).rel, x);
    }
}
