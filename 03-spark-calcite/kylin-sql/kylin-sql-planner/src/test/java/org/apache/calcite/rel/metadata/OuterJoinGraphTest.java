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

import evolution.Debugger;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.graph.OuterJoinGraph;
import org.apache.kylin.test.OuterJoinGraphAssert;
import org.apache.kylin.test.Resource.TPCH;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

@Slf4j
class OuterJoinGraphTest {
    @Test
    void testSimple() {
        final String MODEL_SQL1 =
          "select * \n" +
            "from \n" +
            " tpch.part,\n" +
            " tpch.lineitem\n" +
             "where\n" +
            "  p_partkey = l_partkey and l_orderkey = p_size";

        final String MODEL_SQL_left_1 =
          "select * \n" +
          "from \n" +
          " tpch.lineitem left join \n" +
          " tpch.part on p_partkey = l_partkey and \n" +
          "              l_orderkey = p_size and" +
          "              p_type = 'xx'";

        TPCH.Tester tester = new TPCH.Tester();
        RelNode rel = tester.canonicalize(MODEL_SQL_left_1) ;
        log.info("Before :\n {}", Debugger.toString(rel));
        RelMdJoinGraph graphBuilder = new RelMdJoinGraph();
        OuterJoinGraph graph = graphBuilder.getGraph(rel, rel.getCluster().getMetadataQuery());
        Assertions.assertThat(graph).isNotNull();
        Assertions.assertThat(graph.vertexSet().size()).isEqualTo(2);
        Assertions.assertThat(graph.edgeSet().size()).isEqualTo(1);
    }

    @Test
    void testMatchExact() {
        final String model =
          "select * from tpch.lineitem \n" +
          "left join tpch.orders on l_orderkey = o_orderkey\n" +
          "left join tpch.part on l_partkey = p_partkey";
        final String query =
          "select * from tpch.lineitem \n" +
            "left join tpch.part on l_partkey = p_partkey\n" +
            "left join tpch.orders on l_orderkey = o_orderkey";
        final String query2 =
          "select count(*) from tpch.lineitem \n" +
            "left join tpch.part on l_partkey = p_partkey\n" +
            "left join tpch.orders on l_orderkey = o_orderkey";

        final RelMdJoinGraph graphBuilder = new RelMdJoinGraph();
        TPCH.Tester tester = new TPCH.Tester();
        Function<String, OuterJoinGraph> factory = sql -> getOuterJoinGraph(graphBuilder, tester, sql);
        OuterJoinGraph modelGraph = getOuterJoinGraph(graphBuilder, tester, model);
        Assertions.assertThat(modelGraph).isNotNull();

        OuterJoinGraphAssert
          .assertThat(factory, query)
          .exactMatch(modelGraph);

        OuterJoinGraphAssert
          .assertThat(factory, query2)
          .exactMatch(modelGraph);
    }

    @NotNull
    private OuterJoinGraph getOuterJoinGraph(RelMdJoinGraph graphBuilder, TPCH.Tester tester, String query) {
        RelNode queryRel = tester.canonicalize(query);
        log.info("Plan :\n {}", Debugger.toString(queryRel));
        OuterJoinGraph queryGraph = graphBuilder.getGraph(queryRel, queryRel.getCluster().getMetadataQuery());
        queryGraph.canonicalize();
        return queryGraph;
    }
}
