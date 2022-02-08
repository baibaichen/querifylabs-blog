/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.sql.planner.plan.rules;

import org.apache.kylin.sql.planner.plan.nodes.KylinLimit;
import org.apache.kylin.sql.planner.plan.nodes.LogicalSpark;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;

import org.immutables.value.Value;

/**
 * Rule to convert an {@link Sort} that has
 * {@code offset} or {@code fetch} set to an
 * {@link KylinLimit}
 * on top of a "pure" {@code Sort} that has no offset or fetch.
 *
 * @see KylinRules#KYLIN_LIMIT_RULE
 */
@Value.Enclosing
public class KylinLimitRule extends RelRule<KylinLimitRule.Config> {

  /** Creates an KylinLimitRule. */
  protected KylinLimitRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    if (sort.offset == null && sort.fetch == null) {
      return;
    }
    RelNode input = sort.getInput();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      // Create a sort with the same sort key, but no offset or fetch.
      input = sort.copy(
          sort.getTraitSet(),
          input,
          sort.getCollation(),
          null,
          null);
    }
    call.transformTo(
      KylinLimit.create(
            convert(input, input.getTraitSet().replace(LogicalSpark.INSTANCE)),
            sort.offset,
            sort.fetch));
  }

  /** Rule configuration. */
  @Value.Immutable(singleton=true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableKylinLimitRule.Config.of()
        .withOperandSupplier(b -> b.operand(Sort.class).anyInputs());

    @Override default KylinLimitRule toRule() {
      return new KylinLimitRule(this);
    }
  }
}
