package io.apache.kylin.calcite;

import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

public class KylinPlanner extends PlannerImpl {
    /**
     * Creates a planner. Not a public API; call
     * {@link Frameworks#getPlanner} instead.
     *
     */
    public KylinPlanner(FrameworkConfig config) {
        super(config);
    }
}
