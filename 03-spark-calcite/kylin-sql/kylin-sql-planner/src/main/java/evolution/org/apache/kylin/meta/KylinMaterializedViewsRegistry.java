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
package evolution.org.apache.kylin.meta;

import com.google.common.collect.ImmutableList;
import org.apache.kylin.sql.planner.delegation.PlannerContext;
import org.apache.kylin.sql.planner.plan.nodes.KylinTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * Registry for materialized views. The goal of this cache is to avoid parsing and creating
 * logical plans for the materialized views at query runtime. When a query arrives, we will
 * just need to consult this cache and extract the logical plans for the views (which had
 * already been parsed) from it.
 *
 * <p> Where should this cache live in?
 */
public class KylinMaterializedViewsRegistry {

    /**
     * Create materialized {@link TableScan}
     * @param cluster what does cluster mean?
     * @param table  Repesent materialized table in the spark, it should be {@link KylinModelTable}
     * @return calcite relation operator
     */
    public static RelNode createMaterializedViewScan(
      RelOptCluster cluster,
      RelDataTypeFactory typeFactory,
      List<String> qualifiedTableName,
      Table table) {
        RelOptKylinModelTable relTable =
          new RelOptKylinModelTable(null, table.getRowType(typeFactory), qualifiedTableName, table);
        return KylinTableScan.create(cluster, relTable);
    }

    /**
     * Parses and creates a materialization.
     */
    public KylinRelOptMaterialization createMaterialization(
      PlannerContext plannerContext,
      String viewSql,
      String mvTableName) {
        Util.discard(mvTableName);
        final RelNode queryRel = plannerContext.createParser().rel(viewSql).rel;
        List<String> columnNames = Util.transform(queryRel.getRowType().getFieldList(), RelDataTypeField::getName);
        final KylinModelTable table = new  KylinModelTable(viewSql, RelOptUtil.getFieldTypeList(queryRel.getRowType()), columnNames, null);
        final RelNode viewScan = createMaterializedViewScan(plannerContext.getOptCluster(), plannerContext.getTypeFactory(), ImmutableList.of("x", "y"), table);
        return new KylinRelOptMaterialization(viewScan, queryRel, null, ImmutableList.of("x", "y"));
    }

    public static KylinRelOptMaterialization createMaterialization(
      PlannerContext plannerContext,
      RelNode queryRel,
      List<String> qualifiedTableName) {
        List<String> columnNames =
          Util.transform(queryRel.getRowType().getFieldList(), RelDataTypeField::getName);
        final KylinModelTable table =
          new KylinModelTable("", RelOptUtil.getFieldTypeList(queryRel.getRowType()), columnNames, null);
        final RelNode viewScan = createMaterializedViewScan(plannerContext.getOptCluster(), plannerContext.getTypeFactory(), qualifiedTableName, table);
        return new KylinRelOptMaterialization(viewScan, queryRel, null, qualifiedTableName);
    }

}
