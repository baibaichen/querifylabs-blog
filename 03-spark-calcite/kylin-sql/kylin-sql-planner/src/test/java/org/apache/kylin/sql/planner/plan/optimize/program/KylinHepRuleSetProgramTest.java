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
package org.apache.kylin.sql.planner.plan.optimize.program;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.apache.kylin.sql.planner.calcite.KylinContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KylinHepRuleSetProgram}.
 */
class KylinHepRuleSetProgramTest {

    @Test
    void testBuildKylinHepRuleSetProgram() {

        Assertions.assertNotNull(
          KylinHepRuleSetProgram.Builder.of()
          .add(RuleSets.ofList(
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.CALC_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS
          ))
          .setHepRulesExecutionType(KylinHepRuleSetProgram.HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setMatchLimit(10)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .build()
        );
    }

    @Test
    void testMatchLimitLessThan1() {
        Assertions.assertThrows(IllegalArgumentException.class,
          () -> KylinHepRuleSetProgram.Builder.of().setMatchLimit(0));
    }

    @Test
    void testNullHepMatchOrder() {
        Assertions.assertThrows(NullPointerException.class,
          () -> KylinHepRuleSetProgram.Builder.of().setHepMatchOrder(null));
    }

    @Test
    void testNullHepRulesExecutionType(){
        Assertions.assertThrows(NullPointerException.class,
          () -> KylinHepRuleSetProgram.Builder.of().setHepRulesExecutionType(null));
    }

    @Test()
    void testNullRuleSets() {
        Assertions.assertThrows(NullPointerException.class,
          () -> KylinHepRuleSetProgram.Builder.of().add(null));
    }

    @Test
    void testRuleOperations() {
        final KylinHepRuleSetProgram<KylinContext> program =
          KylinHepRuleSetProgram.Builder.of()
          .add(RuleSets.ofList(
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.CALC_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS
          )).build();

        Assertions.assertTrue(program.contains(CoreRules.FILTER_REDUCE_EXPRESSIONS));
        Assertions.assertTrue(program.contains(CoreRules.PROJECT_REDUCE_EXPRESSIONS));
        Assertions.assertTrue(program.contains(CoreRules.CALC_REDUCE_EXPRESSIONS));
        Assertions.assertTrue(program.contains(CoreRules.JOIN_REDUCE_EXPRESSIONS));
        Assertions.assertFalse(program.contains(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE));

        program.remove(RuleSets.ofList(
          CoreRules.FILTER_REDUCE_EXPRESSIONS,
          CoreRules.PROJECT_REDUCE_EXPRESSIONS));
        Assertions.assertFalse(program.contains(CoreRules.FILTER_REDUCE_EXPRESSIONS));
        Assertions.assertFalse(program.contains(CoreRules.PROJECT_REDUCE_EXPRESSIONS));
        Assertions.assertTrue(program.contains(CoreRules.CALC_REDUCE_EXPRESSIONS));
        Assertions.assertTrue(program.contains(CoreRules.JOIN_REDUCE_EXPRESSIONS));

        program.replaceAll(RuleSets.ofList(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE));
        Assertions.assertFalse(program.contains(CoreRules.CALC_REDUCE_EXPRESSIONS));
        Assertions.assertFalse(program.contains(CoreRules.JOIN_REDUCE_EXPRESSIONS));
        Assertions.assertTrue(program.contains(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE));

        program.add(RuleSets.ofList(
          CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
          CoreRules.JOIN_SUB_QUERY_TO_CORRELATE));
        Assertions.assertTrue(program.contains(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE));
        Assertions.assertTrue(program.contains(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE));
        Assertions.assertTrue(program.contains(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE));
    }
}
