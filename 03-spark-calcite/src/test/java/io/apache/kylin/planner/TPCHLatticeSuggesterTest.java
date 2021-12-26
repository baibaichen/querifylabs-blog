package io.apache.kylin.planner;

import com.google.common.collect.ImmutableList;
import io.apache.kylin.planner.debug.Debugger;
import io.apache.kylin.test.Resource.LatticeHEP;
import io.apache.kylin.test.Resource.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.LatticeRootNode;
import org.apache.calcite.materialize.LatticeSuggester;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@Slf4j
class TPCHLatticeSuggesterTest {

    Optional<Lattice> getFirstLattice(String SQL) throws Exception {
        final Tester t = new Tester().tpch().withEvolve(true);
        t.addQuery(SQL);
        t.suggester.getLatticeSet().forEach(lattice -> log.info(lattice.sql(ImmutableBitSet.of(0), false, ImmutableList.of())));
        return t.suggester.getLatticeSet().stream().findFirst();
    }
    @TestFactory
    Stream<DynamicTest> tpchTest() {
        return IntStream.range(0, 22).
                mapToObj(i -> dynamicTest("LatticeSuggesterTest" + (i + 1), () -> getFirstLattice(Util.QUERIES.get(i))));
    }

    @Test
    void test_lattice_sql1() throws Exception {
        String sql =
            "select\n" +
            "    l_returnflag,\n" +
            "    l_linestatus,\n" +
            "    sum(l_quantity) as sum_qty,\n" +
            "    sum(l_extendedprice) as sum_base_price,\n" +
            "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
            "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
            "    avg(l_quantity) as avg_qty,\n" +
            "    avg(l_extendedprice) as avg_price,\n" +
            "    avg(l_discount) as avg_disc,\n" +
            "    count(*) as count_order\n" +
            "from\n" +
            "    lineitem\n" +
            "    inner join orders on l_orderkey = o_orderkey\n" +
            "    inner join part on l_partkey = p_partkey\n" +
            "    inner join supplier on l_suppkey = s_suppkey\n" +
            "    inner join partsupp on l_partkey = ps_partkey and l_suppkey = ps_suppkey\n" +
            "    inner join customer on o_custkey = c_custkey\n" +
            "    inner join nation cn on c_nationkey = cn.n_nationkey\n" +
            "    inner join nation sn on s_nationkey = sn.n_nationkey\n" +
            "    inner join region cr on cn.n_regionkey = cr.r_regionkey\n" +
            "    inner join region sr on sn.n_regionkey = sr.r_regionkey\n" +
            "where\n" +
            "    l_shipdate <= '1998-09-16'\n" +
            "group by\n" +
            "    l_returnflag,\n" +
            "    l_linestatus\n" +
            "order by\n" +
            "    l_returnflag,\n" +
            "    l_linestatus";
        getFirstLattice(sql);
    }

    @Test
    void test_lattice_sql2() throws Exception {
        String sql =
                "select\n" +
                "    l_returnflag,\n" +
                "    l_linestatus,\n" +
                "    sum(l_quantity) as sum_qty,\n" +
                "    sum(l_extendedprice) as sum_base_price,\n" +
                "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
                "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
                "    avg(l_quantity) as avg_qty,\n" +
                "    avg(l_extendedprice) as avg_price,\n" +
                "    avg(l_discount) as avg_disc,\n" +
                "    count(*) as count_order\n" +
                "from\n" +
                "    lineitem\n" +
                "    inner join orders on l_orderkey = o_orderkey\n" +
                "    inner join partsupp on l_partkey = ps_partkey and l_suppkey = ps_suppkey\n" +
                "    inner join part on p_partkey = ps_partkey\n" +
                "    inner join supplier on ps_suppkey = s_suppkey\n" +
                "    inner join customer on o_custkey = c_custkey\n" +
                "    inner join nation cn on c_nationkey = cn.n_nationkey\n" +
                "    inner join nation sn on s_nationkey = sn.n_nationkey\n" +
                "    inner join region cr on cn.n_regionkey = cr.r_regionkey\n" +
                "    inner join region sr on sn.n_regionkey = sr.r_regionkey\n" +
                "where\n" +
                "    l_shipdate <= '1998-09-16'\n" +
                "group by\n" +
                "    l_returnflag,\n" +
                "    l_linestatus\n" +
                "order by\n" +
                "    l_returnflag,\n" +
                "    l_linestatus";
        final Tester t = new Tester().tpch().withEvolve(true);
        t.doWithFisrtLattice(sql);
    }

    /** Test helper. */
    private static class Tester {
        final LatticeSuggester suggester;
        private final FrameworkConfig config;

        Tester() {
            this(config(CalciteAssert.SchemaSpec.BLANK).build());
        }
        private Tester(FrameworkConfig config) {
            this.config = config;
            suggester = new LatticeSuggester(config);
        }

        Tester withConfig(FrameworkConfig config) {
            return new Tester(config);
        }

        List<Lattice> addQuery(String q) throws SqlParseException,
                ValidationException, RelConversionException {
            final Planner planner = new PlannerImpl(config);
            final SqlNode node = planner.parse(q);
            final SqlNode node2 = planner.validate(node);
            final RelRoot root = planner.rel(node2);
            log.info(LatticeHEP.afterTransformationSQL(root.project()));
            return suggester.addQuery(root.project());
        }
        void doWithFisrtLattice(String query) throws Exception {
            Lattice lattice = null;
            final Planner planner = new PlannerImpl(config);
            {
                final SqlNode node = planner.parse(query);
                final SqlNode node2 = planner.validate(node);
                final RelRoot root = planner.rel(node2);
                List<Lattice> lattices = suggester.addQuery(root.project());
                if (lattices.isEmpty())
                    return;
                lattice = lattices.get(0);
            }
            String sql =
              "select \n" +
              "  l_returnflag,\n" +
              "  l_linestatus,\n" +
              "  sum(l_quantity) as sum_qty \n" +
              "from lineitem\n" +
              "group by\n" +
              "    l_returnflag,\n" +
              "    l_linestatus";
            {
                Optimizer optimizer = Optimizer.of("tpch", Util.TPCH_SCHEMA, ImmutableList.of(lattice));
                final SqlNode node = optimizer.parse(sql);
                final SqlNode node2 = optimizer.validate(node);
                final RelNode root = optimizer.convert(node2);
                RelTraitSet traitSet = root.getTraitSet().replace(EnumerableConvention.INSTANCE);
                RelNode optimized =  optimizer.optimize(root, traitSet, true);
                log.info(Debugger.toSql(optimized));
            }
//            StarTable table = lattice.createStarTable();
//            RelDataType rowType = table.getRowType(planner.getTypeFactory());

        }
        /** Parses a query returns its graph. */
        LatticeRootNode node(String q) throws SqlParseException,
                ValidationException, RelConversionException {
            final List<Lattice> list = addQuery(q);
            assertThat(list.size(), is(1));
            return list.get(0).rootNode;
        }

        static Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec spec) {
            final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, spec);
            return Frameworks.newConfigBuilder()
                    .parserConfig(SqlParser.Config.DEFAULT)
                    .statisticProvider(TPCHStatisticProvider.INSTANCE)
                    .defaultSchema(schema);
        }

        Tester withEvolve(boolean evolve) {
            if (evolve == config.isEvolveLattice()) {
                return this;
            }
            final Frameworks.ConfigBuilder configBuilder =
                    Frameworks.newConfigBuilder(config);
            return new Tester(configBuilder.evolveLattice(true).build());
        }

        Tester tpch() {
            final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            final SchemaPlus schema = rootSchema.add("tpch", Util.TPCH_SCHEMA);
            final FrameworkConfig config = Frameworks.newConfigBuilder()
                    .parserConfig(SqlParser.config().withCaseSensitive(false))
                    .statisticProvider(TPCHStatisticProvider.INSTANCE)
                    .context(
                            Contexts.of(
                                    CalciteConnectionConfig.DEFAULT
                                            .set(CalciteConnectionProperty.CONFORMANCE,
                                                    SqlConformanceEnum.LENIENT.name())))
                    .defaultSchema(schema)
                    .build();
            return withConfig(config);
        }
    }
}
