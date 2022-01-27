package org.example.test;

import io.apache.kylin.spark.calcite.table.planner.delegation.SparkBatchPlanner;
import io.apache.kylin.table.delegation.Parser;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReadTest {

    private transient SparkSession spark;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .getOrCreate();
    }

    @AfterEach
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    void testSimple() {
        String sql =
               "SELECT a, b\n" +
               "FROM spark_catalog.\"default\".t\n" +
               "WHERE a-b > 10\n" +
               "GROUP BY fake-breaker\n" +
               "ORDER BY c";
        String createTable =
                "create table t(a int, b int, fake int, breaker int, c string) using parquet options('compression'='snappy')";
        spark.sql(createTable);

        SparkBatchPlanner planner = new SparkBatchPlanner(spark);

        Parser parser = planner.getParser();
        parser.parse(sql);
    }
}
