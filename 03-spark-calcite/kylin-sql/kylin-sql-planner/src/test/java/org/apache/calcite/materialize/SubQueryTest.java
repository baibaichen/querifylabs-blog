package org.apache.calcite.materialize;

import com.google.common.collect.ImmutableList;
import evolution.Debugger;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.kylin.sql.planner.delegation.PlannerContext;
import org.apache.kylin.test.Resource.LatticeHEP;
import org.apache.kylin.test.Resource.TPCH;
import org.junit.jupiter.api.Test;

import static evolution.org.apache.kylin.calcite.ProgramsCopied.RewriteCorrelatedScalarSubQuery;

@Slf4j
class SubQueryTest {

    @Test
    void test_04() {
        PlannerContext plannerContext = TPCH.newPlannerContext();
        RelNode query = plannerContext.createParser().rel(TPCH.sql(9)).rel;
        log.info("original:\n{}", Debugger.toString(query));
        Program program = RewriteCorrelatedScalarSubQuery();
        RelNode convert =
          program.run(plannerContext.getPlanner(), query, query.getTraitSet(), ImmutableList.of(), ImmutableList.of());
        convert = LatticeHEP.toLeafJoinForm(convert);
        log.info("     sql:\n{}", Debugger.toPostgreSQL(convert));
    }
}
