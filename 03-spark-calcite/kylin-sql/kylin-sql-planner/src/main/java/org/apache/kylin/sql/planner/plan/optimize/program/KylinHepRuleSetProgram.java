package org.apache.kylin.sql.planner.plan.optimize.program;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.sql.planner.calcite.KylinContext;

import java.util.Objects;



/**
 * A RuleSetProgram that runs with {@link org.apache.calcite.plan.hep.HepPlanner HepPlanner}.
 *
 * <p>In most case this program could meet our requirements, otherwise we could choose
 * {@link KylinHepProgram} for some advanced features.
 *
 * <p>Currently, default hep execution type is {@link HEP_RULES_EXECUTION_TYPE#RULE_SEQUENCE}.
 * (Please refer to {@link HEP_RULES_EXECUTION_TYPE} for more info about execution types)
 *
 * @param <OC> OptimizeContext
 */
public class KylinHepRuleSetProgram<OC extends  KylinContext> extends KylinRuleSetProgram<OC> {

    public enum HEP_RULES_EXECUTION_TYPE {
        /**
         * Rules in RULE_SEQUENCE type are executed with RuleInstance.
         * RuleInstance is an instruction that matches a specific rule, each rule in the rule
         * collection is associated with one RuleInstance. Each RuleInstance will be executed
         * only once according to the order defined by the rule collection, but a rule may be applied
         * more than once. If arbitrary order is needed, use RULE_COLLECTION instead.
         *
         * Please refer to {@link HepProgramBuilder#addRuleInstance} for more info about RuleInstance.
         */
        RULE_SEQUENCE,

        /**
         * Rules in RULE_COLLECTION type are executed with RuleCollection.
         * RuleCollection is an instruction that matches any rules in a given collection.
         * The order in which the rules within a collection will be attempted is arbitrary,
         * so if more control is needed, use RULE_SEQUENCE instead.
         *
         * Please refer to {@link HepProgramBuilder#addRuleCollection} for more info about RuleCollection.
         */
        RULE_COLLECTION
    }

    /**
     * The order of graph traversal when looking for rule matches,
     * default match order is ARBITRARY.
     */
    private HepMatchOrder matchOrder  = HepMatchOrder.ARBITRARY;

    /**
     * The limit of pattern matches for this program,
     * default match limit is Integer.MAX_VALUE.
     */
    private Integer matchLimit = Integer.MAX_VALUE;

    /**
     * Hep rule execution type. This is a required item,
     * default execution type is RULE_SEQUENCE.
     */
    private HEP_RULES_EXECUTION_TYPE executionType = HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE;

    /**
     * Requested root traits, this is an optional item.
     */
    private RelTrait[] requestedRootTraits = null;

    @Override
    public RelNode optimize(RelNode input, OC context) {

        if (rules.isEmpty()) {
            return input;
        }

        // build HepProgram
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(matchOrder);
        builder.addMatchLimit(matchLimit);
        switch (executionType) {
            case RULE_SEQUENCE:
                rules.forEach(builder::addRuleInstance);
                break;
            case RULE_COLLECTION:
                builder.addRuleCollection(rules);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported HEP_RULES_EXECUTION_TYPE: " + executionType) ;
        }

        // optimize with HepProgram
        KylinHepProgram<OC> kylinHepProgram = KylinHepProgram.of(builder.build(), requestedRootTraits);
        return kylinHepProgram.optimize(input, context);
    }

    /**
     * Sets rules match order.
     */
    public void setHepMatchOrder(HepMatchOrder matchOrder) {
        this.matchOrder = Objects.requireNonNull(matchOrder);
    }

    /**
     * Sets the limit of pattern matches.
     */
    public void setMatchLimit(Integer matchLimit) {
        Preconditions.checkArgument(matchLimit > 0);
        this.matchLimit = matchLimit;
    }

    /**
     * Sets hep rule execution type.
     */
    public void setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE executionType) {
        this.executionType = Objects.requireNonNull(executionType);
    }

    /**
     * Sets requested root traits.
     */
    public void setRequestedRootTraits(RelTrait[] relTraits) {
        requestedRootTraits = relTraits;
    }

}
