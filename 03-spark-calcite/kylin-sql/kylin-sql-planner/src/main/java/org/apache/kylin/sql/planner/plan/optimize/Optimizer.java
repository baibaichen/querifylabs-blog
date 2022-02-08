package org.apache.kylin.sql.planner.plan.optimize;

import java.util.function.Function;

/**
 * The query [[Optimizer]] that transforms relational expressions into
 * semantically equivalent relational expressions.
 */

public interface Optimizer<T, R> extends Function<T, R> {

    default R optimize(T t) {
        return apply(t);
    }
}
