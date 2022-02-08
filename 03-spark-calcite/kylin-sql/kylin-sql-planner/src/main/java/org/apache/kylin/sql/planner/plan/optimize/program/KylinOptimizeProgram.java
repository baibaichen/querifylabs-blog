package org.apache.kylin.sql.planner.plan.optimize.program;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.sql.planner.calcite.KylinContext;

/**
 * Likes {@link org.apache.calcite.tools.Program}, {@link KylinOptimizeProgram} transforms a relational
 * expression into another relational expression.
 *
 * @param <OC> Optimize Context
 */
public interface KylinOptimizeProgram<OC extends KylinContext> {
    RelNode optimize(RelNode root, OC context);
}
