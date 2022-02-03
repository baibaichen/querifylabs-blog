package org.example.test;

import io.apache.kylin.api.core.Transformation;
import io.apache.kylin.api.table.operations.CollectModifyOperation;
import io.apache.kylin.api.table.operations.QueryOperation;
import io.apache.kylin.pp.spark.calcite.table.planner.delegation.SparkBatchPlanner;
import io.apache.kylin.api.table.delegation.Parser;
import io.apache.kylin.api.table.operations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
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
               // "GROUP BY fake-breaker\n" +
               "ORDER BY c";
        String createTable =
                "create table t(a int, b int, fake int, breaker int, c string) using parquet options('compression'='snappy')";
        spark.sql(createTable);
        SparkBatchPlanner planner = new SparkBatchPlanner(spark);
        Parser parser = planner.getParser();
        List<Operation> operations = parser.parse(sql);
        assertEquals(1, operations.size());
        QueryOperation operation = (QueryOperation) operations.get(0);
        CollectModifyOperation sinkOperation = new CollectModifyOperation(operation);

        List<Transformation<?>> transformations = planner.translate(Collections.singletonList(sinkOperation));

        assertEquals(1, transformations.size());

    }
}
