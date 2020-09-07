package com.sevak_avet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object Runner extends Logging {
  def runTask(sessions: DataFrame, taskNumber: Int): Unit = {
    val methodName = s"task${taskNumber}"
    val method = classOf[SessionsAnalyser]
      .getDeclaredMethods
      .find(m => m.getName.equals(methodName))
      .get

    val instance = new SessionsAnalyser()
    method.invoke(instance, sessions)
  }

  def run(spark: SparkSession, args: Array[String]): Unit = {
    val taskNumber = Integer.parseInt(args(0))
    if (taskNumber < 1 || taskNumber > 8) {
      logError(s"Task #${taskNumber} not found")
      return
    }

    val sessions = spark.read
      .format("bigquery")
      .load("divine-bloom-279918.gridu.sessions")

    runTask(sessions, taskNumber)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Analyse sessions data on Spark")
      .getOrCreate();

    spark.sparkContext.setLogLevel("ERROR")
    run(spark, args)
  }
}
