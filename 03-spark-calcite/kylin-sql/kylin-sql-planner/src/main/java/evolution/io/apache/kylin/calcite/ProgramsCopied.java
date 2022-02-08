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
package evolution.io.apache.kylin.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

import static org.apache.calcite.tools.Programs.sequence;
import static org.apache.calcite.tools.Programs.subQuery;

public class ProgramsCopied {
   private ProgramsCopied() {}

    /** Program that de-correlates a query.
     *
     * <p>To work around
     * <a href="https://issues.apache.org/jira/browse/CALCITE-842">[CALCITE-842]
     * Decorrelator gets field offsets confused if fields have been trimmed</a>,
     * disable field-trimming in {@link SqlToRelConverter}, and run
     * {@link TrimFieldsProgram} after this program. */
    private static class DecorrelateProgram implements Program {
        @Override public RelNode run(
          RelOptPlanner planner, RelNode rel,
          RelTraitSet requiredOutputTraits,
          List<RelOptMaterialization> materializations,
          List<RelOptLattice> lattices) {
            final CalciteConnectionConfig config =
              planner.getContext().maybeUnwrap(CalciteConnectionConfig.class)
                .orElse(CalciteConnectionConfig.DEFAULT);
            if (config.forceDecorrelate()) {
                final RelBuilder relBuilder =
                  RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
                return RelDecorrelator.decorrelateQuery(rel, relBuilder);
            }
            return rel;
        }
    }
    /** Program that trims fields. */
    private static class TrimFieldsProgram implements Program {
        @Override public RelNode run(
          RelOptPlanner planner, RelNode rel,
          RelTraitSet requiredOutputTraits,
          List<RelOptMaterialization> materializations,
          List<RelOptLattice> lattices) {
            final RelBuilder relBuilder =
              RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return new RelFieldTrimmer(null, relBuilder).trim(rel);
        }
    }
    /** Returns the standard program used by Prepare. */
    public static Program standard() {
        return standard(DefaultRelMetadataProvider.INSTANCE);
    }

    /** Returns the standard program with user metadata provider. */
    public static Program standard(RelMetadataProvider metadataProvider) {
        final Program program1 =
          (planner, rel, requiredOutputTraits, materializations, lattices) -> {
              for (RelOptMaterialization materialization : materializations) {
                  planner.addMaterialization(materialization);
              }
              for (RelOptLattice lattice : lattices) {
                  planner.addLattice(lattice);
              }

              planner.setRoot(rel);
              final RelNode rootRel2 =
                rel.getTraitSet().equals(requiredOutputTraits)
                  ? rel
                  : planner.changeTraits(rel, requiredOutputTraits);
              assert rootRel2 != null;

              planner.setRoot(rootRel2);
              final RelOptPlanner planner2 = planner.chooseDelegate();
              final RelNode rootRel3 = planner2.findBestExp();
              assert rootRel3 != null : "could not implement exp";
              return rootRel3;
          };

        return sequence(
          subQuery(metadataProvider),
          new DecorrelateProgram(),
          new TrimFieldsProgram(),
          program1
        );
    }
}
