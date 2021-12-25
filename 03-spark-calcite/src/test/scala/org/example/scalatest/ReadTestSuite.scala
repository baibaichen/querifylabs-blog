package org.example.scalatest

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession


class ReadTestSuite  extends SparkFunSuite with SharedSparkSession {
  // import testImplicits._

  test ("simple") {
    val df = spark.read.parquet("/Users/chang.chen/test/data/tnt.snappy.parquet")
    df.printSchema()
    df.show(false)
  }

}
