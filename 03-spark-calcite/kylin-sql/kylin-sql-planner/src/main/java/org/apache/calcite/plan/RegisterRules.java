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
package org.apache.calcite.plan;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.stream.StreamRules;

import static org.apache.calcite.plan.RelOptUtil.registerAbstractRelationalRules;
import static org.apache.calcite.plan.RelOptUtil.registerAbstractRules;

public class RegisterRules {
    private RegisterRules() {}

    public static void registerDefaultRules(
      RelOptPlanner planner,
      boolean enableMaterialziations,
      boolean enableBindable,
      boolean enableEnumerable) {
        if (Boolean.TRUE.equals(CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value())) {
            registerAbstractRelationalRules(planner);
        }
        registerAbstractRules(planner);
        RelOptRules.BASE_RULES.forEach(planner::addRule);

        if (enableMaterialziations) {
            RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
        }
        if (enableBindable) {
            for (RelOptRule rule : Bindables.RULES) {
                planner.addRule(rule);
            }
        }
        // Registers this rule for default ENUMERABLE convention
        // because:
        // 1. ScannableTable can bind data directly;
        // 2. Only BindableTable supports project push down now.

        // EnumerableInterpreterRule.INSTANCE would then transform
        // the BindableTableScan to
        // EnumerableInterpreter + BindableTableScan.

        // Note: the cost of EnumerableInterpreter + BindableTableScan
        // is always bigger that EnumerableTableScan because of the additional
        // EnumerableInterpreter node, but if there are pushing projects or filter,
        // we prefer BindableTableScan instead,
        // see BindableTableScan#computeSelfCost.
        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        planner.addRule(CoreRules.PROJECT_TABLE_SCAN);
        planner.addRule(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);

        if (enableEnumerable
          && Boolean.TRUE.equals(CalciteSystemProperty.ENABLE_ENUMERABLE.value())) {
            EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);
            planner.addRule(EnumerableRules.TO_INTERPRETER);
        }

        if (enableBindable
          && enableEnumerable
          && Boolean.TRUE.equals(CalciteSystemProperty.ENABLE_ENUMERABLE.value())) {
            planner.addRule(EnumerableRules.TO_BINDABLE);
        }

        if (Boolean.TRUE.equals(CalciteSystemProperty.ENABLE_STREAM.value())) {
            for (RelOptRule rule : StreamRules.RULES) {
                planner.addRule(rule);
            }
        }

        planner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    }
}
