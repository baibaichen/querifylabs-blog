package org.apache.kylin.sql.planner.plan.optimize;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.List;

import static org.apache.kylin.sql.planner.plan.optimize.program.KylinPrograms.hep;

/**
 * Represents a planner phase with its description and a used rule set.
 */
public enum PlannerPhase {

    HEP_REWRITE_SUBQUERY(
      "Heuristic phase to rewrite subqueries",
      CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
      CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
      CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
    ) {
        /** {@inheritDoc} */
        @Override
        public Program getProgram(Object ctx) {
            return hep(getRules(ctx));
        }
    },
    DECORRELATE (""){
        public Program getProgram(Object ctx) {
            return hep(getRules(ctx));
        }
    },
    HEP_CANONICALIZE_RULES_PUSH_FILTER(
      "Heuristic phase to canonicalize relational expression",
      CoreRules.FILTER_PROJECT_TRANSPOSE,
      CoreRules.FILTER_MERGE,
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.JOIN_CONDITION_PUSH,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      CoreRules.PROJECT_MERGE,
      CoreRules.PROJECT_REMOVE,
      CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
      CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,
      CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE
    ) {
        /** {@inheritDoc} */
        @Override
        public Program getProgram(Object ctx) {
            return hep(getRules(ctx));
        }
    },
    HEP_SELECT_MODEL(
      "Heuristic phase to canonicalize relational expression",
      MaterializedViewRules.JOIN
    ) {
        /** {@inheritDoc} */
        @Override
        public Program getProgram(Object ctx) {
            return hep(getRules(ctx));
        }
    };

    public final String description;

    private final List<RelOptRule> rules;

    /**
     * Constructor.
     *
     * @param description A description of the phase.
     * @param rules A list of rules associated with the current phase.
     */
    PlannerPhase(String description, RelOptRule... rules) {
        this.description = description;
        this.rules = ImmutableList.copyOf(rules);
    }

    /**
     * Returns rule set, calculated on the basis of query, planner context and planner phase.
     *
     * @param ctx Planner context.
     * @return Rule set.
     */
    public RuleSet getRules(Object ctx) {
        return RuleSets.ofList(rules);
    }

    /**
     * Returns a program, calculated on the basis of query, planner context planner phase and rules set.
     *
     * @param ctx Planner context.
     * @return Rule set.
     */
    public abstract Program getProgram(Object ctx);
}
