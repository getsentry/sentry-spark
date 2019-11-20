package io.sentry.spark.listener;

import scala.collection.JavaConversions._;

import org.scalatest._;

import org.apache.spark.sql.{SparkSession, AnalysisException};
import org.apache.spark.sql.util.QueryExecutionListener;
import org.apache.spark.sql.execution.QueryExecution

import io.sentry.{Sentry, SentryClient};
import io.sentry.event.{Breadcrumb, Event};
import io.sentry.event.helper.ShouldSendEventCallback;

import io.sentry.spark.testUtil.SentryBaseSpec;

class SentryQueryExecutionListenerSpec extends SentryBaseSpec {
  "SentryQueryExecutionListener" should "set a breadcrumb" in {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Test")
      .config(
        "spark.sql.queryExecutionListeners",
        "io.sentry.spark.listener.SentryQueryExecutionListener"
      )
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    val people = spark.sparkContext.makeRDD(
      """{"name":"Michael"}{"name":"Andy", "age":30}{"name":"Justin", "age":19}""" :: Nil
    )
    val df = spark.read.json(people)

    import spark.implicits._

    df.select("name").show()

    spark.stop();

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include("Query head");
  }
}
