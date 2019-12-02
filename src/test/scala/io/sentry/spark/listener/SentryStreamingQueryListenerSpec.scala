package io.sentry.spark.listener;

import scala.collection.JavaConversions._;

import org.scalatest._;

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.execution.streaming.MemoryStream

import io.sentry.Sentry;

import io.sentry.spark.testUtil.SentryBaseSpec;

class SentryStreamingQueryListenerSpec extends SentryBaseSpec {
  "SentryStreamingQueryListener" should "set a breadcrumb" in {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.streaming.streamingQueryListeners",
        "io.sentry.spark.listener.SentryStreamingQueryListener"
      )
      .getOrCreate()

    import spark.implicits._
    implicit val sqlContext = spark.sqlContext

    val input = List(List(1), List(2, 3))
    def compute(input: Dataset[Int]): Dataset[Int] = {
      input.map(elem => elem + 3)
    }

    val inputStream = MemoryStream[Int]
    val transformed = compute(inputStream.toDS())

    val query =
      transformed.writeStream.format("memory").outputMode("append").queryName("query_name").start()

    input.foreach(batch => inputStream.addData(batch))

    query.processAllAvailable()
    val table = spark.table("query_name").as[Int]
    val resultRows = table.collect()
    assert(resultRows.toSeq === List(4, 5, 6))

    spark.stop()

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 4;

    val firstBreadcrumb = breadcrumbs(0)
    firstBreadcrumb.getMessage() should include("query_name");
    firstBreadcrumb.getMessage() should include("started");
    firstBreadcrumb.getData() should contain key ("runId")

    for (index <- 1 to 3) {
      val breadcrumb = breadcrumbs(index)
      breadcrumb.getMessage() should include("query_name");
      breadcrumb.getMessage() should include("progressed");
      breadcrumb.getData() should contain key ("runId")
      breadcrumb.getData() should contain key ("timestamp")
      breadcrumb.getData() should contain key ("json")
    }
  }
}
