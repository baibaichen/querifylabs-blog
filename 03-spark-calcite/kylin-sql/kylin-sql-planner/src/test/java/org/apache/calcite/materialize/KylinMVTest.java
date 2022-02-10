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
import evolution.org.apache.kylin.meta.KylinMaterializedViewsRegistry;
import evolution.org.apache.kylin.meta.KylinRelOptMaterialization;
import org.apache.kylin.sql.planner.delegation.PlannerContext;
import org.apache.kylin.sql.planner.plan.nodes.KylinTableScan;
import org.apache.kylin.sql.planner.plan.nodes.LogicalSpark;
import org.apache.kylin.test.Resource.TPCH;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RegisterRules;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Programs;
import org.junit.jupiter.api.Test;

import static org.apache.kylin.sql.planner.plan.rules.KylinRules.KYLIN_RULES;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
@Slf4j
class KylinMVTest {
    private static final String VIEW_SQL = TPCH.sql(1);

    @Test
    void test_sql1() {
        final String sql = TPCH.sql(1);
        KylinMaterializedViewsRegistry viewRegistry = new KylinMaterializedViewsRegistry();
        PlannerContext plannerContext = TPCH.newPlannerContext();
        KylinRelOptMaterialization x = viewRegistry.createMaterialization(plannerContext, VIEW_SQL, "x");
        assertNotNull(x);
        assertInstanceOf(KylinTableScan.class, x.tableRel);
        RelNode query = plannerContext.createParser().rel(sql).rel;
        RelNode rewrite = optimize2(query, x);
        log.info("\n" + RelOptUtil.toString(rewrite));
        assertNotNull(rewrite);
    }

    private RelNode optimize2(RelNode queryRel, KylinRelOptMaterialization materialization) {
        RelOptPlanner planner = queryRel.getCluster().getPlanner();
        RelTraitSet traitSet = queryRel.getCluster().traitSet().replace(LogicalSpark.INSTANCE);
        RegisterRules.registerDefaultRules(planner, true, false, false);
        KYLIN_RULES.forEach(planner::addRule);
        return Programs.standard()
                .run(planner, queryRel, traitSet, ImmutableList.of(materialization), ImmutableList.of());
    }
}
