package evolution.org.apache.kylin.calcite;

import com.google.common.collect.ImmutableList;
import evolution.LatticeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RegisterRules;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.DumperWrapper;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Util;
import org.apache.kylin.sql.planner.delegation.PlannerContext;
import org.apache.kylin.sql.planner.parse.CalciteParser;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

import static org.apache.kylin.sql.planner.plan.rules.KylinRules.KYLIN_RULES;

@Slf4j
public class Optimizer {
    private final CalciteParser parser;
    private final VolcanoPlanner planner;
    private final List<RelOptLattice> lattices;

    public Optimizer(PlannerContext plannerContext, List<RelOptLattice> lattices) {
        this.parser = plannerContext.createParser();
        this.planner = plannerContext.getPlanner();
        this.lattices = lattices;
    }
    public static Optimizer of(String name, Schema schema) {
        return of(name, schema, ImmutableList.of(), false);
    }
    public static Optimizer of(String name, Schema schema, List<Lattice> lattices) {
        return of(name, schema, lattices, false);
    }

    public static Optimizer of(String name, Schema schema, List<Lattice> lattices, boolean useKylin) {
        PlannerContext plannerContext =
          new PlannerContext(CalciteSchemaBuilder.createRootSchemaWithChild(schema, name), new JavaTypeFactoryImpl());
        VolcanoPlanner planner = plannerContext.getPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RegisterRules.registerDefaultRules(planner, false, false, !useKylin);
        if (useKylin) {
            KYLIN_RULES.forEach(planner::addRule);
        }

        List<RelOptLattice> relOptLattices =
          Util.transform(lattices,
            lattice -> new RelOptLattice(lattice, LatticeFactory.create(lattice, plannerContext.createCatalogReader(), plannerContext.getTypeFactory())));

        return new Optimizer(plannerContext, relOptLattices);
    }

    public SqlNode parse(String sql) {
       return parser.parse(sql);
    }

    public SqlNode validate(SqlNode node) {
        // nothing
        return node;
    }

    public RelNode convert(SqlNode node) {
        return parser.rel(node).rel;
    }

    public RelNode beforeOptimize(RelNode node, RelTraitSet requiredTraitSet) {
        Program program = Programs.sequence(Programs.subQuery(DefaultRelMetadataProvider.INSTANCE));
        return program.run(planner, node, requiredTraitSet, Collections.emptyList(), Collections.emptyList());
    }

    public RelNode optimize(
      RelNode node,
      RelTraitSet requiredTraitSet,
      boolean dumpGraphviz) {
        Program program = ProgramsCopied.standard();
        RelNode optimized = program.run(planner, node, requiredTraitSet, Collections.emptyList(), lattices);
        if (dumpGraphviz) {
            StringWriter sw = new StringWriter();
            DumperWrapper.dumpGraphviz(planner, new PrintWriter(sw));
            log.info("\n" + sw);
        }
        return optimized;
    }
}
