package io.apache.kylin.pp.spark.calcite.table.planner.delegation;

import com.google.common.collect.ImmutableList;
import io.apache.kylin.api.core.Transformation;
import io.apache.kylin.api.table.operations.ModifyOperation;
import io.apache.kylin.pp.calcite.impl.QueryParser;
import io.apache.kylin.pp.spark.calcite.PlannerContext;
import io.apache.kylin.api.table.delegation.Parser;
import io.apache.kylin.pp.spark.calcite.table.planner.operations.PlannerQueryOperation;
import io.apache.kylin.pp.spark.calcite.table.planner.optimize.CalciteOptimizer;
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
        return new ParserImpl(this::createParser);
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

    private QueryParser createParser() {
        PlannerContext context = getPlannerContext();
        return new QueryParser(context.createSqlValidator(context.createCatalogReader()), context.createCluster());
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
