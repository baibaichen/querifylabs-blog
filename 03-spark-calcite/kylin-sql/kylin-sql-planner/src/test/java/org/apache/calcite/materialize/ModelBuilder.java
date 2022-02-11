package org.apache.calcite.materialize;

import evolution.LatticeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kylin.sql.planner.delegation.PlannerContext;
import org.apache.kylin.test.Resource.TPCH;

public class ModelBuilder {

    public static final String LINEITEM_PART_MODEL = "select 1 from \"tpch\".lineitem inner join \"tpch\".part on l_partkey = p_partkey";

    public static class ContextBuilder {
        private String modelSQL = "";
        private String schemaName = "";
        private String baseViewName = "";

        public ContextBuilder setModelSQL(String baseViewName, String modelSQL) {
            this.modelSQL = modelSQL;
            this.baseViewName = baseViewName;
            return this;
        }

        public ContextBuilder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public PlannerContext buildPlannerContext() {
            CalciteSchema rootSchema = buildRootSchema();
            return new PlannerContext(rootSchema, new JavaTypeFactoryImpl());
        }

        public CalciteSchema buildRootSchema() {
            CalciteSchema rootSchema = CalciteSchemaBuilder.createRootSchemaWithChild(TPCH.SCHEMA, "tpch");
            Lattice.Builder latticeBuilder = Lattice.builder(new LatticeSpace(TPCHStatisticProvider.INSTANCE) ,rootSchema, modelSQL);
            Lattice lattice = latticeBuilder.build();
            CalciteSchema adhoc = rootSchema.add(schemaName, new AbstractSchema());
            adhoc.add(baseViewName, LatticeFactory.createKylinModelTable(lattice));
            return rootSchema;
        }
    }
}
