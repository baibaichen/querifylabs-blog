package org.apache.kylin.test.Resource;

import com.google.common.collect.ImmutableList;
import org.apache.kylin.sql.planner.calcite.KylinPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.hr.Department;
import org.apache.calcite.test.schemata.hr.DepartmentPlus;
import org.apache.calcite.test.schemata.hr.Dependent;
import org.apache.calcite.test.schemata.hr.Employee;
import org.apache.calcite.test.schemata.hr.Event;
import org.apache.calcite.test.schemata.hr.Location;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.mapping.IntPair;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Util {

    @SuppressWarnings("rawtypes")
    static public Planner getPlanner(
      List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig,
      Program... programs) {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return getPlanner(
                traitDefs,
                parserConfig,
                CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR),
                programs);
    }

    @SuppressWarnings("rawtypes")
    static public Planner getPlanner(
      List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig,
      SchemaPlus defaultSchema,
      Program... programs) {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(defaultSchema)
                .traitDefs(traitDefs)
                .programs(programs)
                .build();
        return new KylinPlanner(config);
    }

    /**
     * Hr schema with FK-UK relationship.
     */
    public static class HrFKUKSchema {
        @Override public String toString() {
            return "HrFKUKSchema";
        }

        public final Employee[] emps = {
                new Employee(100, 10, "Bill", 10000, 1000),
                new Employee(200, 20, "Eric", 8000, 500),
                new Employee(150, 10, "Sebastian", 7000, null),
                new Employee(110, 10, "Theodore", 10000, 250),
        };
        public final Department[] depts = {
                new Department(10, "Sales", Arrays.asList(emps[0], emps[2], emps[3]),
                        new Location(-122, 38)),
                new Department(30, "Marketing", ImmutableList.of(),
                        new Location(0, 52)),
                new Department(20, "HR", Collections.singletonList(emps[1]), null),
        };
        public final DepartmentPlus[] depts2 = {
                new DepartmentPlus(10, "Sales", Arrays.asList(emps[0], emps[2], emps[3]),
                        new Location(-122, 38), new Timestamp(0)),
                new DepartmentPlus(30, "Marketing", ImmutableList.of(),
                        new Location(0, 52), new Timestamp(0)),
                new DepartmentPlus(20, "HR", Collections.singletonList(emps[1]),
                        null, new Timestamp(0)),
        };
        public final Dependent[] dependents = {
                new Dependent(10, "Michael"),
                new Dependent(10, "Jane"),
        };
        public final Dependent[] locations = {
                new Dependent(10, "San Francisco"),
                new Dependent(20, "San Diego"),
        };
        public final Event[] events = {
                new Event(100, new Timestamp(0)),
                new Event(200, new Timestamp(0)),
                new Event(150, new Timestamp(0)),
                new Event(110, null),
        };

        public final RelReferentialConstraint rcs0 =
                RelReferentialConstraintImpl.of(
                        ImmutableList.of("hr", "emps"), ImmutableList.of("hr", "depts"),
                        ImmutableList.of(IntPair.of(1, 0)));

        public QueryableTable foo(int count) {
            return Smalls.generateStrings(count);
        }

        public TranslatableTable view(String s) {
            return Smalls.view(s);
        }

        public TranslatableTable matview() {
            return Smalls.strView("noname");
        }
    }
}
