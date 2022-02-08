package org.example.debug

import org.apache.spark.internal.Logging

object ScalaApp extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo("hello")
  }
}
