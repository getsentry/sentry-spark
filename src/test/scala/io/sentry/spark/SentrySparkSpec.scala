package io.sentry.spark;

import scala.collection.JavaConversions._

import org.scalatest._

import org.apache.spark.{SparkContext, SparkConf};

import io.sentry.Sentry;

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

    try {
      testMethod(sparkContext);
    } finally {
      sparkContext.stop();
      Sentry.getContext().clear();
    }
  }
}

class SentrySparkSpec
    extends FlatSpec
    with Matchers
    with PartialFunctionValues
    with SparkContextSetup {
  "SentrySpark.applyContext" should "set tags" in withSparkContext { (sparkContext) =>
    {
      SentrySpark.applyContext(sparkContext);
      val tags = Sentry.getContext().getTags().toMap;

      tags.valueAt("app_name") should equal(sparkContext.appName);
      tags.valueAt("version") should equal(sparkContext.version);
      tags.valueAt("application_id") should equal(sparkContext.applicationId);
      tags.valueAt("master") should equal(sparkContext.master);

      tags.valueAt("driver.host") should equal("localhost");
      tags.valueAt("driver.port") should equal("5674");
      tags.valueAt("executor.id") should equal("driver");
      tags.valueAt("spark-submit.deployMode") should equal("client");
    }
  }

  "SentrySpark.applyContext" should "set a user" in withSparkContext { (sparkContext) =>
    {
      SentrySpark.applyContext(sparkContext);
      val username = Sentry.getContext().getUser().getUsername();

      assert(username == sparkContext.sparkUser)
    }
  }
}
