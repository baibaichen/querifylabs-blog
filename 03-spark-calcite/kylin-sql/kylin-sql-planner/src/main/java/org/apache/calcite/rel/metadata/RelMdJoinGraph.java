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
package org.apache.calcite.rel.metadata;

import evolution.org.apache.kylin.calcite.RelMdExpressionLineageCopied;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.graph.OuterJoinGraph;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class RelMdJoinGraph {

    static final class FindFirstJoin extends RelHomogeneousShuttle {
        @Nullable
        private LogicalJoin join = null;
        @Override
        public RelNode visit(LogicalJoin join){
            this.join = join;
            return join;
        }
    }

    public OuterJoinGraph getGraph(RelNode node, RelMetadataQuery mq) {
        if (node instanceof Filter) {
            return getGraph((Filter) node, mq);
        } else if (node instanceof TableScan) {
            return getGraph((TableScan) node, mq);
        } else if (node instanceof Join) {
            return getGraph((Join) node, mq);
        } else if (node instanceof Aggregate) {
            FindFirstJoin findFirstJoin = new FindFirstJoin();
            node.accept(findFirstJoin);
            if (findFirstJoin.join == null) {
                throw new UnsupportedOperationException("cann't find join");
            }
            return getGraph(findFirstJoin.join, mq);
        }
        throw new UnsupportedOperationException();
    }

    public OuterJoinGraph getGraph(TableScan rel, RelMetadataQuery mq) {
        OuterJoinGraph graph = new OuterJoinGraph();
        graph.addVertex(RexTableInputRef.RelTableRef.of(rel.getTable(), 0));
        return graph;
    }

    public OuterJoinGraph getGraph(Filter filter, RelMetadataQuery mq) {

        OuterJoinGraph graph = getGraph(filter.getInput(), mq);

        // Extract input fields referenced by Filter condition
        RexNode pred = filter.getCondition();
        final ImmutableBitSet inputFieldsUsed = RelOptUtil.InputFinder.bits(pred);

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for (int idx : inputFieldsUsed) {
            final RexInputRef ref = RexInputRef.of(idx, filter.getRowType().getFieldList());
            final Set<RexNode> originalExprs = mq.getExpressionLineage(filter, ref);
            if (originalExprs == null) {
                // Bail out
                return null;
            }
            mapping.put(ref, originalExprs);
        }
        // Replace with new expressions and return merge of Join Graph
        RexConvertToOriginalInput replace = new RexConvertToOriginalInput(mapping);
        graph.addFilter(replace.apply(pred));

        return graph;
    }

    public OuterJoinGraph getGraph(Join join, RelMetadataQuery mq) {
        OuterJoinGraph left = getGraph(join.getLeft(), mq);
        OuterJoinGraph right = getGraph(join.getRight(), mq);

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexNode pred = join.getCondition();

        // Extract input fields referenced by Join condition
        final ImmutableBitSet inputFieldsUsed = RelOptUtil.InputFinder.bits(pred);

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        final RelDataType fullRowType = join.getRowType();
        RelMdExpressionLineageCopied mq_copied = new RelMdExpressionLineageCopied();
        for (int idx : inputFieldsUsed) {
            final RexInputRef inputRef = RexInputRef.of(idx, fullRowType.getFieldList());
            final Set<RexNode> originalExprs = mq_copied.getExpressionLineage(join, mq, inputRef);
            if (originalExprs == null) {
                // Bail out
                return null;
            }
            final RexInputRef ref = RexInputRef.of(idx, fullRowType.getFieldList());
            mapping.put(ref, originalExprs);
        }

        // Replace with new expressions and return merge of Join Graph
        RexConvertToOriginalInput replace = new RexConvertToOriginalInput(mapping);
        RexNode updated = replace.apply(pred);
        return OuterJoinGraph.merge(rexBuilder, left, right, RelOptUtil.conjunctions(updated));
    }
}
