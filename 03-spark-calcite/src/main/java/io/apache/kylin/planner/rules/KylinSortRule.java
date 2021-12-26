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
package io.apache.kylin.planner.rules;

import io.apache.kylin.planner.debug.Debugger;
import io.apache.kylin.planner.nodes.KylinSort;
import io.apache.kylin.planner.nodes.LogicalSpark;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert an {@link Sort} to an
 * {@link KylinSort}.
 *
 * @see KylinRules#KYLIN_SORT_RULE
 */
@Slf4j
class KylinSortRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(Sort.class, Convention.NONE,
          LogicalSpark.INSTANCE, "KylinSortRule")
      .withRuleFactory(KylinSortRule::new);

  /** Called from the Config. */
  protected KylinSortRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final Sort sort = (Sort) rel;
    if (sort.offset != null || sort.fetch != null) {
      return null;
    }
    final RelNode input = sort.getInput();
    return KylinSort.create(
      convert(input, input.getTraitSet().replace(LogicalSpark.INSTANCE)),
      sort.getCollation(),
      null,
      null);
  }
}
