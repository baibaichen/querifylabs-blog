package org.apache.kylin.sql.planner.calcite;


import org.apache.calcite.plan.Context;

/**
 * A {@link  Context} to allow the store data within the planner session and access it within rules.
 */
public interface KylinContext extends Context {
}
