package io.sentry.spark;

import scala.collection.JavaConverters._;

import org.scalatest._

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.{StreamingContext, Seconds};
import org.apache.spark.{SparkContext, SparkConf};

import io.sentry.{Sentry, Scope, ScopeCallback};

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkContext) => Any) {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark Test")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")
      .set("spark.submit.deployMode", "client")
      .set("spark.driver.port", "5674");

    val sparkContext = new SparkContext(sparkConf);
    sparkContext.setLogLevel("ERROR")

    try {
      testMethod(sparkContext);
    } finally {
      sparkContext.stop();
      Sentry.configureScope((scope: Scope) => {
        scope.clear();
      }: ScopeCallback);
    }
  }

  def withSparkSession(testMethod: (SparkSession) => Any) {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark Test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.submit.deployMode", "client")
      .config("spark.driver.port", "5674")
      .getOrCreate();

    sparkSession.sparkContext.setLogLevel("ERROR");

    try {
      testMethod(sparkSession);
    } finally {
      sparkSession.stop();
      Sentry.configureScope((scope: Scope) => {
        scope.clear();
      }: ScopeCallback);
    }
  }

  def withStreamingContext(testMethod: (StreamingContext) => Any) {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark Test")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")
      .set("spark.submit.deployMode", "client")
      .set("spark.driver.port", "5674");

    val streamingContext = new StreamingContext(sparkConf, Seconds(1));
    streamingContext.sparkContext.setLogLevel("ERROR")

    try {
      testMethod(streamingContext);
    } finally {
      streamingContext.stop();
      Sentry.configureScope((scope: Scope) => {
        scope.clear();
      }: ScopeCallback);
    }
  }
}

class SentrySparkSpec
    extends FlatSpec
    with Matchers
    with PartialFunctionValues
    with SparkContextSetup {
  "SentrySpark.applyContext" should "set SparkContext tags" in withSparkContext { (sparkContext) =>
    {
      SentrySpark.applyContext(sparkContext);
      checkTags(sparkContext);
    }
  }

  "SentrySpark.applyContext" should "set SparkSession tags" in withSparkSession { (sparkSession) =>
    {
      SentrySpark.applyContext(sparkSession);
      checkTags(sparkSession.sparkContext);
    }
  }

  "SentrySpark.applyContext" should "set StreamingContext tags" in withStreamingContext {
    (streamingContext) =>
      {
        SentrySpark.applyContext(streamingContext);
        checkTags(streamingContext.sparkContext);
      }
  }

  def checkTags(sparkContext: SparkContext) {
    Sentry.configureScope((scope: Scope) => {
      val tags = scope.getTags().asScala;

      tags.valueAt("version") should equal(sparkContext.version);
      tags.valueAt("application_id") should equal(sparkContext.applicationId);
      tags.valueAt("master") should equal(sparkContext.master);

      tags.valueAt("driver.host") should equal("localhost");
      tags.valueAt("driver.port") should equal("5674");
      tags.valueAt("executor.id") should equal("driver");
      tags.valueAt("spark-submit.deployMode") should equal("client");

      val username = scope.getUser().getUsername();
      assert(username == sparkContext.sparkUser)
    }: ScopeCallback);
  }
}
