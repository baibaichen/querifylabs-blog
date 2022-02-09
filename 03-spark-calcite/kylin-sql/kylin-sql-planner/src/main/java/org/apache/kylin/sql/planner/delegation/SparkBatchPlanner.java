package org.apache.kylin.sql.planner.delegation;

import com.google.common.collect.ImmutableList;
import org.apache.kylin.core.Transformation;
import org.apache.kylin.sql.planner.operations.PlannerQueryOperation;
import org.apache.kylin.sql.planner.plan.optimize.CalciteOptimizer;
import org.apache.kylin.table.operations.ModifyOperation;
import org.apache.kylin.table.delegation.Parser;
import org.apache.calcite.rel.RelNode;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class SparkBatchPlanner extends PlannerBase {
    private final CalciteOptimizer optimizer;

    public SparkBatchPlanner(SparkSession session) {
        super(session);
        optimizer = new CalciteOptimizer();
    }

    @Override
    public Parser getParser() {
        return new ParserImpl(plannerContext::createParser);
    }

    @Override
    public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
        if (modifyOperations.isEmpty()) {
            return ImmutableList.of();
        }
        return modifyOperations.stream()
                .map(this::translateToRel)
                .map(relNode -> getOptimizer().optimize(relNode))
                .map(Transformation::new)
                .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CalciteOptimizer getOptimizer() {
        return optimizer;
    }
    private RelNode translateToRel(ModifyOperation modifyOperation) {
        PlannerQueryOperation qo = (PlannerQueryOperation) modifyOperation.getChild();
        return qo.getCalciteTree();
    }
}
