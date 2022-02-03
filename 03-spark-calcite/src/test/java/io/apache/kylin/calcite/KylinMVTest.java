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
package io.apache.kylin.calcite;

import com.google.common.collect.ImmutableList;
import evolution.io.apache.kylin.calcite.KylinPlannerContext;
import evolution.io.apache.kylin.calcite.cost.KylinVolcanoPlanner;
import evolution.io.apache.kylin.meta.KylinMaterializedViewsRegistry;
import evolution.io.apache.kylin.meta.KylinRelOptMaterialization;
import io.apache.kylin.pp.calcite.nodes.KylinTableScan;
import io.apache.kylin.pp.calcite.nodes.LogicalSpark;
import io.apache.kylin.test.Resource.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RegisterRules;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Programs;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.apache.kylin.pp.calcite.rules.KylinRules.KYLIN_RULES;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
@Slf4j
class KylinMVTest {
    private static final String VIEW_SQL = Util.QUERIES.get(0);

    @Test
    void test_sql1() throws SqlParseException {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        SchemaPlus root = rootSchema.plus();
        final SchemaPlus defaultSchema = root.add("tpch", Util.TPCH_SCHEMA);

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        final String sql = Util.QUERIES.get(0);

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);
        final RelOptPlanner planner = KylinVolcanoPlanner.createPlanner(new KylinPlannerContext(config));
        final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        KylinMaterializedViewsRegistry viewRegistry = new KylinMaterializedViewsRegistry(config);
        KylinRelOptMaterialization x =
          viewRegistry.createMaterialization(cluster, typeFactory, defaultSchema, VIEW_SQL, "x");
        assertNotNull(x);
        assertInstanceOf(KylinTableScan.class, x.tableRel);
        RelNode query = viewRegistry.toRel(cluster, typeFactory, defaultSchema, sql);
        RelNode rewrite = optimize2(query, x);
        log.info("\n" + RelOptUtil.toString(rewrite));
        assertNotNull(rewrite);
    }

    private RelNode optimize2(RelNode queryRel, KylinRelOptMaterialization materialization) {
        RelOptPlanner planner = queryRel.getCluster().getPlanner();
        RelTraitSet traitSet = queryRel.getCluster().traitSet().replace(LogicalSpark.INSTANCE);
        RegisterRules.registerDefaultRules(planner, true, false, false);
        KYLIN_RULES.forEach(planner::addRule);
        return Programs
          .standard()
          .run(planner, queryRel, traitSet, ImmutableList.of(materialization), ImmutableList.of());
    }
}
