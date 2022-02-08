package org.apache.kylin.sql.planner.plan.nodes;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.util.Util;

/** Calling convention for relational operations that occur in Kylin. */
public enum LogicalSpark implements Convention {
    INSTANCE;

    @Override public String toString() {
        return getName();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class getInterface() {
        return LogicalSparkRel.class;
    }

    @Override
    public String getName() {
        return "LogicalSpark";
    }

    @Override
    @SuppressWarnings("rawtypes")
    public RelTraitDef getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return this == trait;
    }

    @Override
    public void register(RelOptPlanner planner) {
        Util.discard(planner);
    }
}