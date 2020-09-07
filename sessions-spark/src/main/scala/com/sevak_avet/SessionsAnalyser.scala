package com.sevak_avet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class SessionsAnalyser extends Logging {

  def task1(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // What is the total number of transactions generated per device.browser
    // date BETWEEN '20170801' AND '20180801' for browsers Chrome, Safari, Firefox

    val filteredSessions = sessions
      .filter($"device.browser".isin("Chrome", "Safari", "Firefox"))
      .filter($"date".between("20170801", "20180801"))

    filteredSessions
      .groupBy($"device.browser")
      .sum("totals.transactions").as("total")
      .show()
  }

  def task2(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // What was the real bounce rate per traffic source for  youtube.com, facebook.com
    // BETWEEN '20170801' AND '20180801'.
    // The real bounce rate is defined as the percentage of visits with a single pageview.

    val filteredSessions = sessions
      .filter($"trafficSource.source".isin("youtube.com", "facebook.com"))
      .filter($"date".between("20170801", "20180801"))

    filteredSessions
      .groupBy($"trafficSource.source")
      .agg(
        count("*").as("total_count"),
        count(when($"totals.pageviews" === 1, true)).as("single_pageview")
      )
      .select(
        $"total_count",
        $"single_pageview",
        $"single_pageview" / $"total_count".cast("double")
      )
      .show()
  }

  def task3(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // What was the average number of product pageviews for users
    // who made a purchase BETWEEN '20170801' AND '20180801'.
    // purchase when totals.transactions >=1.

    val filteredSessions = sessions
      .filter($"totals.transactions" > 0)
      .filter($"date".between("20170801", "20180801"))

    filteredSessions
      .agg(avg($"totals.pageviews").as("avg_pageviews"))
      .show()
  }

  def task4(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // What was the average number of product pageviews for users
    // who did not make a purchase BETWEEN '20170801' AND '20180801'.

    val filteredSessions = sessions
      .filter(($"totals.transactions" isNull) || ($"totals.transactions" === 0))
      .filter($"date".between("20170801", "20180801"))

    filteredSessions
      .agg(avg($"totals.pageviews").as("avg_pageviews"))
      .show()
  }

  def task5(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // What was the average total transactions per user
    // that made a purchase BETWEEN '20170801' AND '20180801'.

    val filteredSessions = sessions
      .filter($"totals.transactions" > 0)
      .filter($"date".between("20170801", "20180801"))

    filteredSessions
      .agg(avg($"totals.transactions").as("avg_transactions"))
      .show()
  }

  def task6(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // What is the average amount of money
    // spent per session BETWEEN '20170801' AND '20180801'.

    val filteredSessions = sessions
      .filter($"totals.transactions" > 0)
      .filter($"totals.visits" > 0)
      .filter($"date".between("20170801", "20180801"))

    filteredSessions
      .groupBy("fullVisitorId")
      .agg(sum($"totals.transactionRevenue").as("revenue"))
      .agg(avg("revenue")).as("avg_revenue")
      .show()
  }

  def task7(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // What is the first 4 sequence of pages
    // viewed BETWEEN '20170801' AND '20180801'?
    // hits.type = 'page' for every record

    sessions
      .withColumn("hits", explode($"hits"))
      .filter($"hits.type" === "PAGE")
      .orderBy($"date".asc)
      .select($"hits.page.pagePath".as("path"))
      .limit(4)
      .show(truncate = false)
  }

  def task8(sessions: DataFrame): Unit = {
    import sessions.sqlContext.implicits._
    // Find Products purchased by customers who purchased product A?
    // product A : hits.product.productSKU = 'GGOEGAEC033115'
    // purchased : hits.eCommerceAction.action_type = '6'

    val hits = sessions
      .withColumn("hit", explode($"hits"))
      .cache()

    val users = hits
      .withColumn("product", explode($"hit.product"))
      .filter($"product.productSKU" === "9182785")
      .withColumnRenamed("hit", "userHit")

    users.join(hits, "fullVisitorId")
      .filter($"hit.eCommerceAction.action_type" === 6)
      .withColumn("product", explode($"hit.product"))
      .select("product.productSKU")
      .distinct
      .show()
  }
}