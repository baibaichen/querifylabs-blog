package org.apache.kylin.sql.planner.plan.optimize.program;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.kylin.sql.planner.calcite.KylinContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A Kylin OptimizeProgram that transforms a relational expression into
 * another relational expression with {@link RuleSet}.
 */
abstract public class KylinRuleSetProgram<OC extends KylinContext> implements KylinOptimizeProgram<OC> {

    /**
     * All {@link RelOptRule}s for optimizing associated with this program.
     */
    protected final List<RelOptRule> rules = new ArrayList<>();

    public void add(RuleSet ruleSet) {
        Objects.requireNonNull(ruleSet);
        ruleSet.forEach(rule -> {
            if (!contains(rule)) {
                rules.add(rule);
            }
        }
        );
    }

    /**
     * Removes specified rules from this program.
     */
    public void remove(RuleSet ruleSet) {
        Objects.requireNonNull(ruleSet);
        ruleSet.forEach(rules::remove);
    }

    /**
     * Removes all rules from this program first, and then adds specified rules to this program.
     */
    public void replaceAll(RuleSet ruleSet) {
        Objects.requireNonNull(ruleSet);
        rules.clear();
        ruleSet.forEach(rules::add);
    }

    /**
     * Checks whether this program contains the specified rule.
     */
    public Boolean contains(RelOptRule rule) {
        Objects.requireNonNull(rule);
        return rules.contains(rule);
    }
}
