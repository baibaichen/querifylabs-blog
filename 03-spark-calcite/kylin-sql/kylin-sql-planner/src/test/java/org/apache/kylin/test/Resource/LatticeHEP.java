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
package org.apache.kylin.test.Resource;

import com.google.common.collect.ImmutableList;
import evolution.Debugger;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.kylin.sql.planner.plan.rules.KylinRules;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

@Slf4j
public class LatticeHEP {
    private static final HepProgram PROGRAM =
            new HepProgramBuilder()
                    .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                    .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
                    .build();

    private static final HepProgram CANONICALIZE_PROGRAM =
      new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_MERGE)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
        .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS)
//        .addRuleInstance(CoreRules.FILTER_TO_CALC)
//        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
//        .addRuleInstance(CoreRules.FILTER_CALC_MERGE)
//        .addRuleInstance(CoreRules.PROJECT_CALC_MERGE)
//        .addRuleInstance(CoreRules.CALC_MERGE)
        .build();

    public static String afterTransformationSQL(RelNode r) {
        final HepPlanner planner =
                new HepPlanner(PROGRAM, null, true, null, RelOptCostImpl.FACTORY);
        planner.setRoot(r);
        final RelNode r2 = planner.findBestExp();
        return Debugger.toSparkSql(r2);
    }


    public static RelNode substituteCanonicalize(RelNode rel) {
        final HepPlanner hepPlanner = new HepPlanner(CANONICALIZE_PROGRAM);
        hepPlanner.setRoot(rel);
        return hepPlanner.findBestExp();
    }

    /**
     * Similar with {@link org.apache.calcite.plan.RelOptMaterialization#toLeafJoinForm},
     * but use {@link KylinRules#CANONICALIZE_RULES}
     */
    public static RelNode toLeafJoinForm(RelNode rel) {
        final Program program = Programs.hep(KylinRules.CANONICALIZE_RULES, false,
                DefaultRelMetadataProvider.INSTANCE);
        log.info("Before :\n {}", Debugger.toString(rel));
        final RelNode rel2 = program.run(castNonNull(null), rel, castNonNull(null),
                ImmutableList.of(),
                ImmutableList.of());
        log.info(" After :\n {}", Debugger.toString(rel2));
        return rel2;
    }
}
