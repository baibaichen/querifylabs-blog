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
package org.apache.calcite.util.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import java.util.List;
import java.util.Map;
import java.util.Set;


public class OuterJoinGraph extends DefaultDirectedGraphCopid<RelTableRef, OuterJoinGraph.OuterJoinEdge> {

    /**
     * Creates a graph.
     *
     */
    public OuterJoinGraph() {
        this(OuterJoinEdge.factory());
    }
    public OuterJoinGraph(EdgeFactory<RelTableRef, OuterJoinEdge> edgeFactory) {
        super(edgeFactory);
    }

    OuterJoinEdge.Factory joinEdgeFactory() {
        return (OuterJoinEdge.Factory) edgeFactory;
    }

    public static class OuterJoinEdge extends DefaultEdge implements Comparable<OuterJoinEdge> {
        private final RexNode joinCondition;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OuterJoinEdge)) return false;
            if (!super.equals(o)) return false;

            OuterJoinEdge that = (OuterJoinEdge) o;

            return joinCondition.equals(that.joinCondition);
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + joinCondition.hashCode();
            return result;
        }

        public OuterJoinEdge(RelTableRef source, RelTableRef target, RexNode joinCondition) {
            super(source, target);
            this.joinCondition = joinCondition;
        }

        public RelTableRef source() {
            return (RelTableRef) source;
        }
        public RelTableRef target() {
            return (RelTableRef) target;
        }
        public RexNode getJoinCondition() {
            return joinCondition;
        }

        @Override
        public int compareTo(@NotNull OuterJoinGraph.OuterJoinEdge o) {
            int compare = source().compareTo(o.source());
            if (compare == 0)
                compare = target().compareTo(o.target());
            return compare;
        }

        /** Creates {@link OuterJoinEdge} instances. */
        static class Factory
          implements DirectedGraph.EdgeFactory<RelTableRef, OuterJoinEdge> {
            static Factory defaultInstance = new Factory();
            @Override
            public OuterJoinEdge createEdge(RelTableRef source, RelTableRef target) {
                throw new UnsupportedOperationException();
            }
            public OuterJoinEdge createEdge(RelTableRef source, RelTableRef target,
              RexNode joinCondition) {
                return new OuterJoinEdge(source, target, joinCondition);
            }
        }

        public static Factory factory() {
            return Factory.defaultInstance;
        }
    }

    /** Predicates that can be pulled up from the relational expression and its
     * inputs. */
    private final List<RexNode> pulledUpPredicates = new ArrayList<>();

    /** Predicates that were got from the input.
     * Empty if the relational expression doesn't have a Filter.
     *
     * <P/>
     * we put it into {@link #tablePredicates} in the {@link #merge}, if it is at right side.
     * */
    private final List<RexNode> temporaryPredicates = new ArrayList<>();

    private final Map<RelTableRef, RexNode> tablePredicates = new HashMap<>(10);

    public OuterJoinEdge addEdge(RelTableRef source, RelTableRef target, RexNode joinCondition) {
        final DefaultDirectedGraphCopid.VertexInfo<RelTableRef, OuterJoinEdge> info = getVertex(source);
        final DefaultDirectedGraphCopid.VertexInfo<RelTableRef, OuterJoinEdge> targetInfo = getVertex(target);
        final OuterJoinEdge edge =  joinEdgeFactory().createEdge(source, target, joinCondition);
        if (edges.add(edge)) {
            info.outEdges.add(edge);
            targetInfo.inEdges.add(edge);
            return edge;
        } else {
            return null;
        }
    }

    public void addFilter(RexNode f) {
        temporaryPredicates.add(f);
    }

    /**
     * prepare for exact match
     */
    public void canonicalize() {
        vertexMap.forEach((relTableRef, relTableRefOuterJoinEdgeVertexInfo) ->{
            Collections.sort(relTableRefOuterJoinEdgeVertexInfo.outEdges);
            Collections.sort(relTableRefOuterJoinEdgeVertexInfo.inEdges);
        } );
    }
    public static boolean exactMatch(OuterJoinGraph left, OuterJoinGraph right) {
        return left.vertexMap.equals(right.vertexMap);
    }

    public static OuterJoinGraph
    merge(RexBuilder rexBuilder, OuterJoinGraph left, OuterJoinGraph right, List<RexNode> joinConjunctions) {

        Preconditions.checkArgument(right.pulledUpPredicates.isEmpty());
        Preconditions.checkArgument(right.temporaryPredicates.size() < 2);
        Preconditions.checkArgument(!joinConjunctions.isEmpty());

        OuterJoinGraph jg = new OuterJoinGraph();

        // left
        Set<RelTableRef> leftSet = left.vertexSet();
        leftSet.forEach(jg::addVertex);
        if (!left.pulledUpPredicates.isEmpty()) {
            Preconditions.checkArgument(left.temporaryPredicates.isEmpty());
            jg.pulledUpPredicates.addAll(left.pulledUpPredicates);
        } else if (!left.temporaryPredicates.isEmpty()) {
            jg.pulledUpPredicates.addAll(left.temporaryPredicates);
        }
        jg.tablePredicates.putAll(left.tablePredicates);
        left.edgeSet().forEach(edge -> jg.addEdge(edge.source(), edge.target(), edge.getJoinCondition()));

        // right
        right.vertexSet().forEach(relTableRef -> {
            Preconditions.checkArgument(!leftSet.contains(relTableRef));
            jg.addVertex(relTableRef);
        });

        if (!right.temporaryPredicates.isEmpty()) {
            List<RelTableRef> tables =
              ImmutableList.copyOf(RexUtil.gatherTableReferences(right.temporaryPredicates));
            Preconditions.checkArgument(tables.size() == 1);
            jg.tablePredicates.put(tables.get(0), right.temporaryPredicates.get(0));
        }
        right.edgeSet().forEach(edge -> jg.addEdge(edge.source(), edge.target(), edge.getJoinCondition()));

        // new edge
        Map<Pair<RelTableRef, RelTableRef>, RexNode> edgeMap = new HashMap<>(10);
        for (RexNode conjunction : joinConjunctions) {
            List<RelTableRef> tables =
              ImmutableList.copyOf(RexUtil.gatherTableReferences(ImmutableList.of(conjunction)));
            if (tables.size() != 2)
                throw new UnsupportedOperationException();

            Pair<RelTableRef, RelTableRef> edge;
            if (leftSet.contains(tables.get(0))) {
                edge = Pair.of(tables.get(0), tables.get(1));
            } else {
                edge = Pair.of(tables.get(0), tables.get(1));
            }

            if (edgeMap.containsKey(edge)) {
                RexNode newPred =
                  RexUtil.composeConjunction(rexBuilder, ImmutableList.of(edgeMap.get(edge), conjunction));
                edgeMap.replace(edge, newPred);
            } else {
                edgeMap.put(edge, conjunction);
            }
        }

        edgeMap.forEach((relTableRefRelTableRefPair, rexNode) -> {
            assert relTableRefRelTableRefPair.left != null;
            assert relTableRefRelTableRefPair.right != null;
            jg.addEdge(relTableRefRelTableRefPair.left, relTableRefRelTableRefPair.right, rexNode);
        });
        return jg;
    }

}
