package org.apache.kylin.test.Resource;

import com.google.common.collect.ImmutableList;
import org.apache.kylin.sql.planner.calcite.KylinPlanner;
import org.apache.calcite.adapter.tpch.TpchSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema;
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
    public static final List<String> QUERIES = ImmutableList.of(
            // 01
            "select\n"
                    + "  l_returnflag\n"
                    + ", l_linestatus\n"
                    + ", sum(l_quantity) as sum_qty\n"
                    + ", sum(l_extendedprice) as sum_base_price\n"
                    + ", sum(l_extendedprice * (1 - l_discount)) as sum_disc_price\n"
                    + ", sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge\n"
                    + ", avg(l_quantity) as avg_qty\n"
                    + ", avg(l_extendedprice) as avg_price\n"
                    + ", avg(l_discount) as avg_disc\n"
                    + ", count(*) as count_order\n"
                    + "from\n"
                    + "  tpch.lineitem\n"
                    + " where\n"
                    + "  l_shipdate <= date '1998-12-01' - interval '120' day (3)\n"
                    + "group by\n"
                    + "  l_returnflag,\n"
                    + "  l_linestatus\n"
                    + "order by\n"
                    + "  l_returnflag,\n"
                    + "  l_linestatus",

            // 02
            "select\n"
                    + "  s.s_acctbal,\n"
                    + "  s.s_name,\n"
                    + "  n.n_name,\n"
                    + "  p.p_partkey,\n"
                    + "  p.p_mfgr,\n"
                    + "  s.s_address,\n"
                    + "  s.s_phone,\n"
                    + "  s.s_comment\n"
                    + "from\n"
                    + "  tpch.part p,\n"
                    + "  tpch.supplier s,\n"
                    + "  tpch.partsupp ps,\n"
                    + "  tpch.nation n,\n"
                    + "  tpch.region r\n"
                    + "where\n"
                    + "  p.p_partkey = ps.ps_partkey\n"
                    + "  and s.s_suppkey = ps.ps_suppkey\n"
                    + "  and p.p_size = 41\n"
                    + "  and p.p_type like '%NICKEL'\n"
                    + "  and s.s_nationkey = n.n_nationkey\n"
                    + "  and n.n_regionkey = r.r_regionkey\n"
                    + "  and r.r_name = 'EUROPE'\n"
                    + "  and ps.ps_supplycost = (\n"
                    + "\n"
                    + "    select\n"
                    + "      min(ps.ps_supplycost)\n"
                    + "\n"
                    + "    from\n"
                    + "      tpch.partsupp ps,\n"
                    + "      tpch.supplier s,\n"
                    + "      tpch.nation n,\n"
                    + "      tpch.region r\n"
                    + "    where\n"
                    + "      p.p_partkey = ps.ps_partkey\n"
                    + "      and s.s_suppkey = ps.ps_suppkey\n"
                    + "      and s.s_nationkey = n.n_nationkey\n"
                    + "      and n.n_regionkey = r.r_regionkey\n"
                    + "      and r.r_name = 'EUROPE'\n"
                    + "  )\n"
                    + "\n"
                    + "order by\n"
                    + "  s.s_acctbal desc,\n"
                    + "  n.n_name,\n"
                    + "  s.s_name,\n"
                    + "  p.p_partkey\n"
                    + "limit 100",

            // 03
            "select\n"
                    + "  l.l_orderkey,\n"
                    + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
                    + "  o.o_orderdate,\n"
                    + "  o.o_shippriority\n"
                    + "\n"
                    + "from\n"
                    + "  tpch.customer c,\n"
                    + "  tpch.orders o,\n"
                    + "  tpch.lineitem l\n"
                    + "\n"
                    + "where\n"
                    + "  c.c_mktsegment = 'HOUSEHOLD'\n"
                    + "  and c.c_custkey = o.o_custkey\n"
                    + "  and l.l_orderkey = o.o_orderkey\n"
                    + "  and o.o_orderdate < date '1995-03-25'\n"
                    + "  and l.l_shipdate > date '1995-03-25'\n"
                    + "\n"
                    + "group by\n"
                    + "  l.l_orderkey,\n"
                    + "  o.o_orderdate,\n"
                    + "  o.o_shippriority\n"
                    + "order by\n"
                    + "  revenue desc,\n"
                    + "  o.o_orderdate\n"
                    + "limit 10",

            // 04
            "select\n"
                    + "  o_orderpriority,\n"
                    + "  count(*) as order_count\n"
                    + "from\n"
                    + "  tpch.orders\n"
                    + "\n"
                    + "where\n"
                    + "  o_orderdate >= date '1996-10-01'\n"
                    + "  and o_orderdate < date '1996-10-01' + interval '3' month\n"
                    + "  and\n"
                    + "  exists (\n"
                    + "    select\n"
                    + "      *\n"
                    + "    from\n"
                    + "      tpch.lineitem\n"
                    + "    where\n"
                    + "      l_orderkey = o_orderkey\n"
                    + "      and l_commitdate < l_receiptdate\n"
                    + "  )\n"
                    + "group by\n"
                    + "  o_orderpriority\n"
                    + "order by\n"
                    + "  o_orderpriority",

            // 05
            "select\n"
                    + "  n.n_name,\n"
                    + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue\n"
                    + "\n"
                    + "from\n"
                    + "  tpch.customer c,\n"
                    + "  tpch.orders o,\n"
                    + "  tpch.lineitem l,\n"
                    + "  tpch.supplier s,\n"
                    + "  tpch.nation n,\n"
                    + "  tpch.region r\n"
                    + "\n"
                    + "where\n"
                    + "  c.c_custkey = o.o_custkey\n"
                    + "  and l.l_orderkey = o.o_orderkey\n"
                    + "  and l.l_suppkey = s.s_suppkey\n"
                    + "  and c.c_nationkey = s.s_nationkey\n"
                    + "  and s.s_nationkey = n.n_nationkey\n"
                    + "  and n.n_regionkey = r.r_regionkey\n"
                    + "  and r.r_name = 'EUROPE'\n"
                    + "  and o.o_orderdate >= date '1997-01-01'\n"
                    + "  and o.o_orderdate < date '1997-01-01' + interval '1' year\n"
                    + "group by\n"
                    + "  n.n_name\n"
                    + "\n"
                    + "order by\n"
                    + "  revenue desc",

            // 06
            "select\n"
                    + "  sum(l_extendedprice * l_discount) as revenue\n"
                    + "from\n"
                    + "  tpch.lineitem\n"
                    + "where\n"
                    + "  l_shipdate >= date '1997-01-01'\n"
                    + "  and l_shipdate < date '1997-01-01' + interval '1' year\n"
                    + "  and\n"
                    + "  l_discount between 0.03 - 0.01 and 0.03 + 0.01\n"
                    + "  and l_quantity < 24",

            // 07
            "select\n"
                    + "  supp_nation,\n"
                    + "  cust_nation,\n"
                    + "  l_year,\n"
                    + "  sum(volume) as revenue\n"
                    + "from\n"
                    + "  (\n"
                    + "    select\n"
                    + "      n1.n_name as supp_nation,\n"
                    + "      n2.n_name as cust_nation,\n"
                    + "      extract(year from l.l_shipdate) as l_year,\n"
                    + "      l.l_extendedprice * (1 - l.l_discount) as volume\n"
                    + "    from\n"
                    + "      tpch.supplier s,\n"
                    + "      tpch.lineitem l,\n"
                    + "      tpch.orders o,\n"
                    + "      tpch.customer c,\n"
                    + "      tpch.nation n1,\n"
                    + "      tpch.nation n2\n"
                    + "    where\n"
                    + "      s.s_suppkey = l.l_suppkey\n"
                    + "      and o.o_orderkey = l.l_orderkey\n"
                    + "      and c.c_custkey = o.o_custkey\n"
                    + "      and s.s_nationkey = n1.n_nationkey\n"
                    + "      and c.c_nationkey = n2.n_nationkey\n"
                    + "      and (\n"
                    + "        (n1.n_name = 'EGYPT' and n2.n_name = 'UNITED STATES')\n"
                    + "        or (n1.n_name = 'UNITED STATES' and n2.n_name = 'EGYPT')\n"
                    + "      )\n"
                    + "      and l.l_shipdate between date '1995-01-01' and date '1996-12-31'\n"
                    + "  ) as shipping\n"
                    + "group by\n"
                    + "  supp_nation,\n"
                    + "  cust_nation,\n"
                    + "  l_year\n"
                    + "order by\n"
                    + "  supp_nation,\n"
                    + "  cust_nation,\n"
                    + "  l_year",

            // 08
            "select\n"
                    + "  o_year,\n"
                    + "  sum(case\n"
                    + "    when nation = 'EGYPT' then volume\n"
                    + "    else 0\n"
                    + "  end) as mkt_share\n"
                    + "from\n"
                    + "  (\n"
                    + "    select\n"
                    + "      extract(year from o.o_orderdate) as o_year,\n"
                    + "      l.l_extendedprice * (1 - l.l_discount) as volume,\n"
                    + "      n2.n_name as nation\n"
                    + "    from\n"
                    + "      tpch.part p,\n"
                    + "      tpch.supplier s,\n"
                    + "      tpch.lineitem l,\n"
                    + "      tpch.orders o,\n"
                    + "      tpch.customer c,\n"
                    + "      tpch.nation n1,\n"
                    + "      tpch.nation n2,\n"
                    + "      tpch.region r\n"
                    + "    where\n"
                    + "      p.p_partkey = l.l_partkey\n"
                    + "      and s.s_suppkey = l.l_suppkey\n"
                    + "      and l.l_orderkey = o.o_orderkey\n"
                    + "      and o.o_custkey = c.c_custkey\n"
                    + "      and c.c_nationkey = n1.n_nationkey\n"
                    + "      and n1.n_regionkey = r.r_regionkey\n"
                    + "      and r.r_name = 'MIDDLE EAST'\n"
                    + "      and s.s_nationkey = n2.n_nationkey\n"
                    + "      and o.o_orderdate between date '1995-01-01' and date '1996-12-31'\n"
                    + "      and p.p_type = 'PROMO BRUSHED COPPER'\n"
                    + "  ) as all_nations\n"
                    + "group by\n"
                    + "  o_year\n"
                    + "order by\n"
                    + "  o_year",

            // 09
            "select\n"
                    + "  nation,\n"
                    + "  o_year,\n"
                    + "  sum(amount) as sum_profit\n"
                    + "from\n"
                    + "  (\n"
                    + "    select\n"
                    + "      n_name as nation,\n"
                    + "      extract(year from o_orderdate) as o_year,\n"
                    + "      l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount\n"
                    + "    from\n"
                    + "      tpch.part p,\n"
                    + "      tpch.supplier s,\n"
                    + "      tpch.lineitem l,\n"
                    + "      tpch.partsupp ps,\n"
                    + "      tpch.orders o,\n"
                    + "      tpch.nation n\n"
                    + "    where\n"
                    + "      s.s_suppkey = l.l_suppkey\n"
                    + "      and ps.ps_suppkey = l.l_suppkey\n"
                    + "      and ps.ps_partkey = l.l_partkey\n"
                    + "      and p.p_partkey = l.l_partkey\n"
                    + "      and o.o_orderkey = l.l_orderkey\n"
                    + "      and s.s_nationkey = n.n_nationkey\n"
                    + "      and p.p_name like '%yellow%'\n"
                    + "  ) as profit\n"
                    + "group by\n"
                    + "  nation,\n"
                    + "  o_year\n"
                    + "order by\n"
                    + "  nation,\n"
                    + "  o_year desc",

            // 10
            "select\n"
                    + "  c.c_custkey,\n"
                    + "  c.c_name,\n"
                    + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
                    + "  c.c_acctbal,\n"
                    + "  n.n_name,\n"
                    + "  c.c_address,\n"
                    + "  c.c_phone,\n"
                    + "  c.c_comment\n"
                    + "from\n"
                    + "  tpch.customer c,\n"
                    + "  tpch.orders o,\n"
                    + "  tpch.lineitem l,\n"
                    + "  tpch.nation n\n"
                    + "where\n"
                    + "  c.c_custkey = o.o_custkey\n"
                    + "  and l.l_orderkey = o.o_orderkey\n"
                    + "  and o.o_orderdate >= date '1994-03-01'\n"
                    + "  and o.o_orderdate < date '1994-03-01' + interval '3' month\n"
                    + "  and l.l_returnflag = 'R'\n"
                    + "  and c.c_nationkey = n.n_nationkey\n"
                    + "group by\n"
                    + "  c.c_custkey,\n"
                    + "  c.c_name,\n"
                    + "  c.c_acctbal,\n"
                    + "  c.c_phone,\n"
                    + "  n.n_name,\n"
                    + "  c.c_address,\n"
                    + "  c.c_comment\n"
                    + "order by\n"
                    + "  revenue desc\n"
                    + "limit 20",

            // 11
            "select\n"
                    + "  ps.ps_partkey,\n"
                    + "  sum(ps.ps_supplycost * ps.ps_availqty) as \"value\"\n"
                    + "from\n"
                    + "  tpch.partsupp ps,\n"
                    + "  tpch.supplier s,\n"
                    + "  tpch.nation n\n"
                    + "where\n"
                    + "  ps.ps_suppkey = s.s_suppkey\n"
                    + "  and s.s_nationkey = n.n_nationkey\n"
                    + "  and n.n_name = 'JAPAN'\n"
                    + "group by\n"
                    + "  ps.ps_partkey having\n"
                    + "    sum(ps.ps_supplycost * ps.ps_availqty) > (\n"
                    + "      select\n"
                    + "        sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001000000 as xxxxxxx\n"
                    + "      from\n"
                    + "        tpch.partsupp ps,\n"
                    + "        tpch.supplier s,\n"
                    + "        tpch.nation n\n"
                    + "      where\n"
                    + "        ps.ps_suppkey = s.s_suppkey\n"
                    + "        and s.s_nationkey = n.n_nationkey\n"
                    + "        and n.n_name = 'JAPAN'\n"
                    + "    )\n"
                    + "order by\n"
                    + "  \"value\" desc",

            // 12
            "select\n"
                    + "  l.l_shipmode,\n"
                    + "  sum(case\n"
                    + "    when o.o_orderpriority = '1-URGENT'\n"
                    + "      or o.o_orderpriority = '2-HIGH'\n"
                    + "      then 1\n"
                    + "    else 0\n"
                    + "  end) as high_line_count,\n"
                    + "  sum(case\n"
                    + "    when o.o_orderpriority <> '1-URGENT'\n"
                    + "      and o.o_orderpriority <> '2-HIGH'\n"
                    + "      then 1\n"
                    + "    else 0\n"
                    + "  end) as low_line_count\n"
                    + "from\n"
                    + "  tpch.orders o,\n"
                    + "  tpch.lineitem l\n"
                    + "where\n"
                    + "  o.o_orderkey = l.l_orderkey\n"
                    + "  and l.l_shipmode in ('TRUCK', 'REG AIR')\n"
                    + "  and l.l_commitdate < l.l_receiptdate\n"
                    + "  and l.l_shipdate < l.l_commitdate\n"
                    + "  and l.l_receiptdate >= date '1994-01-01'\n"
                    + "  and l.l_receiptdate < date '1994-01-01' + interval '1' year\n"
                    + "group by\n"
                    + "  l.l_shipmode\n"
                    + "order by\n"
                    + "  l.l_shipmode",

            // 13
            "select\n"
                    + "  c_count,\n"
                    + "  count(*) as custdist\n"
                    + "from\n"
                    + "  (\n"
                    + "    select\n"
                    + "      c.c_custkey,\n"
                    + "      count(o.o_orderkey)\n"
                    + "    from\n"
                    + "      tpch.customer c\n"
                    + "      left outer join tpch.orders o\n"
                    + "        on c.c_custkey = o.o_custkey\n"
                    + "        and o.o_comment not like '%special%requests%'\n"
                    + "    group by\n"
                    + "      c.c_custkey\n"
                    + "  ) as orders (c_custkey, c_count)\n"
                    + "group by\n"
                    + "  c_count\n"
                    + "order by\n"
                    + "  custdist desc,\n"
                    + "  c_count desc",

            // 14
            "select\n"
                    + "  100.00 * sum(case\n"
                    + "    when p.p_type like 'PROMO%'\n"
                    + "      then l.l_extendedprice * (1 - l.l_discount)\n"
                    + "    else 0\n"
                    + "  end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue\n"
                    + "from\n"
                    + "  tpch.lineitem l,\n"
                    + "  tpch.part p\n"
                    + "where\n"
                    + "  l.l_partkey = p.p_partkey\n"
                    + "  and l.l_shipdate >= date '1994-08-01'\n"
                    + "  and l.l_shipdate < date '1994-08-01' + interval '1' month",

            // 15
            "with revenue0 (supplier_no, total_revenue) as (\n"
                    + "  select\n"
                    + "    l_suppkey,\n"
                    + "    sum(l_extendedprice * (1 - l_discount))\n"
                    + "  from\n"
                    + "    tpch.lineitem\n"
                    + "  where\n"
                    + "    l_shipdate >= date '1993-05-01'\n"
                    + "    and l_shipdate < date '1993-05-01' + interval '3' month\n"
                    + "  group by\n"
                    + "    l_suppkey)\n"
                    + "select\n"
                    + "  s.s_suppkey,\n"
                    + "  s.s_name,\n"
                    + "  s.s_address,\n"
                    + "  s.s_phone,\n"
                    + "  r.total_revenue\n"
                    + "from\n"
                    + "  tpch.supplier s,\n"
                    + "  revenue0 r\n"
                    + "where\n"
                    + "  s.s_suppkey = r.supplier_no\n"
                    + "  and r.total_revenue = (\n"
                    + "    select\n"
                    + "      max(total_revenue)\n"
                    + "    from\n"
                    + "      revenue0\n"
                    + "  )\n"
                    + "order by\n"
                    + "  s.s_suppkey",

            // 16
            "select\n"
                    + "  p.p_brand,\n"
                    + "  p.p_type,\n"
                    + "  p.p_size,\n"
                    + "  count(distinct ps.ps_suppkey) as supplier_cnt\n"
                    + "from\n"
                    + "  tpch.partsupp ps,\n"
                    + "  tpch.part p\n"
                    + "where\n"
                    + "  p.p_partkey = ps.ps_partkey\n"
                    + "  and p.p_brand <> 'Brand#21'\n"
                    + "  and p.p_type not like 'MEDIUM PLATED%'\n"
                    + "  and p.p_size in (38, 2, 8, 31, 44, 5, 14, 24)\n"
                    + "  and ps.ps_suppkey not in (\n"
                    + "    select\n"
                    + "      s_suppkey\n"
                    + "    from\n"
                    + "      tpch.supplier\n"
                    + "    where\n"
                    + "      s_comment like '%Customer%Complaints%'\n"
                    + "  )\n"
                    + "group by\n"
                    + "  p.p_brand,\n"
                    + "  p.p_type,\n"
                    + "  p.p_size\n"
                    + "order by\n"
                    + "  supplier_cnt desc,\n"
                    + "  p.p_brand,\n"
                    + "  p.p_type,\n"
                    + "  p.p_size",

            // 17
            "select\n"
                    + "  sum(l.l_extendedprice) / 7.0 as avg_yearly\n"
                    + "from\n"
                    + "  tpch.lineitem l,\n"
                    + "  tpch.part p\n"
                    + "where\n"
                    + "  p.p_partkey = l.l_partkey\n"
                    + "  and p.p_brand = 'Brand#13'\n"
                    + "  and p.p_container = 'JUMBO CAN'\n"
                    + "  and l.l_quantity < (\n"
                    + "    select\n"
                    + "      0.2 * avg(l2.l_quantity)\n"
                    + "    from\n"
                    + "      tpch.lineitem l2\n"
                    + "    where\n"
                    + "      l2.l_partkey = p.p_partkey\n"
                    + "  )",

            // 18
            "select\n"
                    + "  c.c_name,\n"
                    + "  c.c_custkey,\n"
                    + "  o.o_orderkey,\n"
                    + "  o.o_orderdate,\n"
                    + "  o.o_totalprice,\n"
                    + "  sum(l.l_quantity)\n"
                    + "from\n"
                    + "  tpch.customer c,\n"
                    + "  tpch.orders o,\n"
                    + "  tpch.lineitem l\n"
                    + "where\n"
                    + "  o.o_orderkey in (\n"
                    + "    select\n"
                    + "      l_orderkey\n"
                    + "    from\n"
                    + "      tpch.lineitem\n"
                    + "    group by\n"
                    + "      l_orderkey having\n"
                    + "        sum(l_quantity) > 313\n"
                    + "  )\n"
                    + "  and c.c_custkey = o.o_custkey\n"
                    + "  and o.o_orderkey = l.l_orderkey\n"
                    + "group by\n"
                    + "  c.c_name,\n"
                    + "  c.c_custkey,\n"
                    + "  o.o_orderkey,\n"
                    + "  o.o_orderdate,\n"
                    + "  o.o_totalprice\n"
                    + "order by\n"
                    + "  o.o_totalprice desc,\n"
                    + "  o.o_orderdate\n"
                    + "limit 100",

            // 19
            "select\n"
                    + "  sum(l.l_extendedprice* (1 - l.l_discount)) as revenue\n"
                    + "from\n"
                    + "  tpch.lineitem l,\n"
                    + "  tpch.part p\n"
                    + "where\n"
                    + "  (\n"
                    + "    p.p_partkey = l.l_partkey\n"
                    + "    and p.p_brand = 'Brand#41'\n"
                    + "    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                    + "    and l.l_quantity >= 2 and l.l_quantity <= 2 + 10\n"
                    + "    and p.p_size between 1 and 5\n"
                    + "    and l.l_shipmode in ('AIR', 'AIR REG')\n"
                    + "    and l.l_shipinstruct = 'DELIVER IN PERSON'\n"
                    + "  )\n"
                    + "  or\n"
                    + "  (\n"
                    + "    p.p_partkey = l.l_partkey\n"
                    + "    and p.p_brand = 'Brand#13'\n"
                    + "    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                    + "    and l.l_quantity >= 14 and l.l_quantity <= 14 + 10\n"
                    + "    and p.p_size between 1 and 10\n"
                    + "    and l.l_shipmode in ('AIR', 'AIR REG')\n"
                    + "    and l.l_shipinstruct = 'DELIVER IN PERSON'\n"
                    + "  )\n"
                    + "  or\n"
                    + "  (\n"
                    + "    p.p_partkey = l.l_partkey\n"
                    + "    and p.p_brand = 'Brand#55'\n"
                    + "    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
                    + "    and l.l_quantity >= 23 and l.l_quantity <= 23 + 10\n"
                    + "    and p.p_size between 1 and 15\n"
                    + "    and l.l_shipmode in ('AIR', 'AIR REG')\n"
                    + "    and l.l_shipinstruct = 'DELIVER IN PERSON'\n"
                    + "  )",

            // 20
            "select\n"
                    + "  s.s_name,\n"
                    + "  s.s_address\n"
                    + "from\n"
                    + "  tpch.supplier s,\n"
                    + "  tpch.nation n\n"
                    + "where\n"
                    + "  s.s_suppkey in (\n"
                    + "    select\n"
                    + "      ps.ps_suppkey\n"
                    + "    from\n"
                    + "      tpch.partsupp ps\n"
                    + "    where\n"
                    + "      ps. ps_partkey in (\n"
                    + "        select\n"
                    + "          p.p_partkey\n"
                    + "        from\n"
                    + "          tpch.part p\n"
                    + "        where\n"
                    + "          p.p_name like 'antique%'\n"
                    + "      )\n"
                    + "      and ps.ps_availqty > (\n"
                    + "        select\n"
                    + "          0.5 * sum(l.l_quantity)\n"
                    + "        from\n"
                    + "          tpch.lineitem l\n"
                    + "        where\n"
                    + "          l.l_partkey = ps.ps_partkey\n"
                    + "          and l.l_suppkey = ps.ps_suppkey\n"
                    + "          and l.l_shipdate >= date '1993-01-01'\n"
                    + "          and l.l_shipdate < date '1993-01-01' + interval '1' year\n"
                    + "      )\n"
                    + "  )\n"
                    + "  and s.s_nationkey = n.n_nationkey\n"
                    + "  and n.n_name = 'KENYA'\n"
                    + "order by\n"
                    + "  s.s_name",

            // 21
            "select\n"
                    + "  s.s_name,\n"
                    + "  count(*) as numwait\n"
                    + "from\n"
                    + "  tpch.supplier s,\n"
                    + "  tpch.lineitem l1,\n"
                    + "  tpch.orders o,\n"
                    + "  tpch.nation n\n"
                    + "where\n"
                    + "  s.s_suppkey = l1.l_suppkey\n"
                    + "  and o.o_orderkey = l1.l_orderkey\n"
                    + "  and o.o_orderstatus = 'F'\n"
                    + "  and l1.l_receiptdate > l1.l_commitdate\n"
                    + "  and exists (\n"
                    + "    select\n"
                    + "      *\n"
                    + "    from\n"
                    + "      tpch.lineitem l2\n"
                    + "    where\n"
                    + "      l2.l_orderkey = l1.l_orderkey\n"
                    + "      and l2.l_suppkey <> l1.l_suppkey\n"
                    + "  )\n"
                    + "  and not exists (\n"
                    + "    select\n"
                    + "      *\n"
                    + "    from\n"
                    + "      tpch.lineitem l3\n"
                    + "    where\n"
                    + "      l3.l_orderkey = l1.l_orderkey\n"
                    + "      and l3.l_suppkey <> l1.l_suppkey\n"
                    + "      and l3.l_receiptdate > l3.l_commitdate\n"
                    + "  )\n"
                    + "  and s.s_nationkey = n.n_nationkey\n"
                    + "  and n.n_name = 'BRAZIL'\n"
                    + "group by\n"
                    + "  s.s_name\n"
                    + "order by\n"
                    + "  numwait desc,\n"
                    + "  s.s_name\n"
                    + "limit 100",

            // 22
            "select\n"
                    + "  cntrycode,\n"
                    + "  count(*) as numcust,\n"
                    + "  sum(c_acctbal) as totacctbal\n"
                    + "from\n"
                    + "  (\n"
                    + "    select\n"
                    + "      substring(c_phone from 1 for 2) as cntrycode,\n"
                    + "      c_acctbal\n"
                    + "    from\n"
                    + "      tpch.customer c\n"
                    + "    where\n"
                    + "      substring(c_phone from 1 for 2) in\n"
                    + "        ('24', '31', '11', '16', '21', '20', '34')\n"
                    + "      and c_acctbal > (\n"
                    + "        select\n"
                    + "          avg(c_acctbal)\n"
                    + "        from\n"
                    + "          tpch.customer\n"
                    + "        where\n"
                    + "          c_acctbal > 0.00\n"
                    + "          and substring(c_phone from 1 for 2) in\n"
                    + "            ('24', '31', '11', '16', '21', '20', '34')\n"
                    + "      )\n"
                    + "      and not exists (\n"
                    + "        select\n"
                    + "          *\n"
                    + "        from\n"
                    + "          tpch.orders o\n"
                    + "        where\n"
                    + "          o.o_custkey = c.c_custkey\n"
                    + "      )\n"
                    + "  ) as custsale\n"
                    + "group by\n"
                    + "  cntrycode\n"
                    + "order by\n"
                    + "  cntrycode");
    static final double SCALE_FACTOR = 0.01d;
    public static final Schema TPCH_SCHEMA = new TpchSchema(SCALE_FACTOR, 1, 1, false);

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
