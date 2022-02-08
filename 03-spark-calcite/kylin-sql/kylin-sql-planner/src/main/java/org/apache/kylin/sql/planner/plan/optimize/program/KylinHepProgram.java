package org.apache.kylin.sql.planner.plan.optimize.program;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.KylinSQLException;
import org.apache.kylin.sql.planner.calcite.KylinContext;

import java.util.Objects;

/**
 * A {@link KylinOptimizeProgram} that runs with {@link HepPlanner}.
 *
 * <p>In most case, {@link KylinHepRuleSetProgram} could meet our requirements.
 * Otherwise we could choose this program for some advanced features,
 * and use {@link org.apache.calcite.plan.hep.HepProgramBuilder} to create {@link HepProgram}.
 *
 * @param <OC> OptimizeContext
 */
public class KylinHepProgram<OC extends KylinContext> implements KylinOptimizeProgram<OC> {

    /**
     * {@link HepProgram}.instance for {@link HepPlanner},
     * this must not be None when doing optimize.
     */
    private HepProgram hepProgram = null;

    /**
     * Requested root traits, it's an optional item.
     */
    private RelTrait[] requestedRootTraits = null;

    @Override
    public RelNode optimize(RelNode root, OC context) {

        if (hepProgram == null) {
            throw KylinSQLException.error(KylinSQLException.ErrorCode.INTERNAL,
                    "hepProgram should not be None in KylinHepProgram");
        }

        HepPlanner planner = new HepPlanner(hepProgram, context);

        // TODO: FlinkRelMdNonCumulativeCost

        planner.setRoot(root);
        if (requestedRootTraits != null) {
            RelTraitSet targetTraitSet = root.getTraitSet().plusAll(requestedRootTraits);
            if(!root.getTraitSet().equals(targetTraitSet)) {
                planner.changeTraits(root, targetTraitSet.simplify());
            }
        }
        return planner.findBestExp();
    }

    /**
     * Sets hep program instance.
     */
    public void setHepProgram(HepProgram hepProgram) {
        this.hepProgram = Objects.requireNonNull(hepProgram);
    }

    /**
     * Sets requested root traits.
     */
    public void setRequestedRootTraits(RelTrait[] requestedRootTraits) {
        this.requestedRootTraits = requestedRootTraits;
    }

    public static<OC extends KylinContext> KylinHepProgram<OC>
    of(HepProgram hepProgram, RelTrait[] requestedRootTraits) {
        KylinHepProgram<OC> kylinHepProgram = new KylinHepProgram<>();
        kylinHepProgram.setHepProgram(hepProgram);
        kylinHepProgram.setRequestedRootTraits(requestedRootTraits);
        return  kylinHepProgram;
    }
}
