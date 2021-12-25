package io.apache.kylin.planner.nodes;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

/** Calling convention for relational operations that occur in Kylin. */
public enum KylinConvention implements Convention {
    INSTANCE;

    @Override public String toString() {
        return getName();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class getInterface() {
        return KylinRel.class;
    }

    @Override
    public String getName() {
        return "KYLIGENCE";
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

    }
}