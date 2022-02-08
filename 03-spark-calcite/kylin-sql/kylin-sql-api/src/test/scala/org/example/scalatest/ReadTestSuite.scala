package org.example.scalatest

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession


class ReadTestSuite  extends SparkFunSuite with SharedSparkSession {
  // import testImplicits._
  import org.apache.spark.sql.catalyst.parser.CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  test ("simple") {
    val sqlText : String = """SELECT a, b
                             |FROM t
                             |WHERE a-b > 10
                             |GROUP BY fake-breaker
                             |ORDER BY c""".stripMargin

    val createTable : String =
      "create table t(a int, b int, fake int, breaker int, c string) using parquet options('compression'='snappy')"

    val parsedPlan = parsePlan(sqlText)
    spark.sql(createTable)
    val plan = table("t")
      .where('a - 'b > 10)
      // .groupBy('fake - 'breaker)('a, 'b)
      .orderBy('c.asc)
    val plan1 = spark.sessionState.analyzer.execute(plan)
    spark.sessionState.analyzer.checkAnalysis(plan1)

    //    val df = spark.read.parquet("/Users/chang.chen/test/data/tnt.snappy.parquet")
    //    df.printSchema()
    //    df.show(false)
  }
}
