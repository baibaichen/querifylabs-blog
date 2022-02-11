package org.apache.kylin.sql.planner.plan.optimize;

import evolution.Debugger;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.kylin.sql.planner.calcite.KylinContext;
import org.apache.kylin.sql.planner.parse.CalciteParser;
import org.apache.kylin.sql.planner.plan.optimize.program.KylinHepRuleSetProgram;
import org.apache.kylin.sql.planner.plan.rules.JoinFilterTransposeRule;
import org.apache.kylin.sql.planner.plan.rules.KylinRules;
import org.apache.kylin.test.Resource.LatticeHEP;
import org.apache.kylin.test.Resource.TPCH;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

@Slf4j
class CanonicalizeTest {
    @Test
    void testCanonicalize2() {
        final CalciteParser parser = TPCH.createCalciteParser();
        String sql2 = "select l.l_suppkey, count(*) from (select l_suppkey, l_partkey, l_linenumber + 2 as x from tpch.lineitem) l inner join tpch.part p on l.l_partkey = p.p_partkey where l.x > 1000 group by l.l_suppkey";
        final RelNode node = parser.rel(sql2).rel;
        log.info("before:\n{}", Debugger.toString(node));

        RelNode node2 = LatticeHEP.substituteCanonicalize(node);
        log.info("after:\n{}", Debugger.toString(node2));
        log.info("after:\n{}", Debugger.toSparkSql(node2));
    }

    @Test
    void testCanonicalize1(){
        final CalciteParser parser = TPCH.createCalciteParser();

        String sql =
                "SELECT t.L_SUPPKEY l_suppkey, COUNT(*)\n" +
                        "FROM (SELECT *\n" +
                        "FROM tpch.LINEITEM\n" +
                        "WHERE L_LINENUMBER > 1000) t\n" +
                        "INNER JOIN tpch.PART ON t.L_PARTKEY = PART.P_PARTKEY\n" +
                        "GROUP BY t.L_SUPPKEY";

        String sql2 = "select l.l_suppkey, count(*) from (select l_suppkey, l_partkey, l_linenumber + 2 as x from tpch.lineitem) l inner join tpch.part p on l.l_partkey = p.p_partkey where l.x > 1000 group by l.l_suppkey";
        String sql3 = "select * from (select *, l_linenumber + 2 as x from tpch.lineitem) l  where l.x > 1000";
        String sql4 = "select l.x, count(*) from (select l_suppkey, l_partkey, l_linenumber + 2 as x from tpch.lineitem) l inner join tpch.part p on l.l_partkey = p.p_partkey where l.x > 1000 group by l.x";
        final RelNode node = parser.rel(TPCH.sql(9)).rel;
        log.info("before:\n{}", Debugger.toString(node));

        KylinContext context = new KylinContext() {
            @Override
            public <C> @Nullable C unwrap(Class<C> aClass) {
                return null;
            }
        };
        final KylinHepRuleSetProgram<KylinContext> program =
                KylinHepRuleSetProgram.Builder.of()
                        .add(RuleSets.ofList(KylinRules.CANONICALIZE_RULES))
                        .setHepRulesExecutionType(KylinHepRuleSetProgram.HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
                        .build();

        program.add(RuleSets.ofList(
                // JoinFilterTransposeRule.Config.LEFT.toRule()
                CoreRules.FILTER_INTO_JOIN
                , CoreRules.JOIN_CONDITION_PUSH
                )
        );

        RelNode node2 = program.optimize(node, context);
        log.info("after:\n{}", Debugger.toString(node2));
        log.info("after:\n{}", Debugger.toPostgreSQL(node2));

        RuleSet pullFilter = RuleSets.ofList(
                JoinFilterTransposeRule.Config.LEFT.toRule()
        );
        final KylinHepRuleSetProgram<KylinContext> program2 =
                KylinHepRuleSetProgram.Builder.of()
                        .add(pullFilter)
                        .setHepRulesExecutionType(KylinHepRuleSetProgram.HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
                        .build();

        RelNode node3 = program2.optimize(node2, context);
        log.info("after pull:\n{}", Debugger.toString(node3));
        log.info("after pull:\n{}", Debugger.toPostgreSQL(node3));
    }

}
