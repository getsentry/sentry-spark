package io.sentry.spark;

import scala.util.{Try, Success, Failure}

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.SparkContext;

import io.sentry.{Sentry};
import io.sentry.event.UserBuilder;

object SentrySpark {
  def init() {
    Sentry.init();
  }

  def applyContext(sparkContext: SparkContext) {
    this.setSparkContextTags(sparkContext);
  }

  def applyContext(sparkSession: SparkSession) {
    this.setSparkContextTags(sparkSession.sparkContext)
  }

  def applyContext(ssc: StreamingContext) {
    this.setSparkContextTags(ssc.sparkContext)
  }

  def setSparkContextTags(sc: SparkContext) {
    Sentry
      .getContext()
      .setUser(
        new UserBuilder().setUsername(sc.sparkUser).build()
      );

    Sentry.getContext().addTag("version", sc.version);
    Sentry.getContext().addTag("app_name", sc.appName);
    Sentry.getContext().addTag("application_id", sc.applicationId);
    Sentry.getContext().addTag("master", sc.master);

    val sparkConf = sc.getConf;

    val tags: List[(String, String)] = List(
      ("spark-submit.deployMode", "spark.submit.deployMode"),
      ("executor.id", "spark.executor.id"),
      ("driver.host", "spark.driver.host"),
      ("driver.port", "spark.driver.port")
    );

    def getConf(value: String) = Try {
      sparkConf.get(value)
    }

    for ((key, value) <- tags) getConf(value) match {
      case Success(configValue) => Sentry.getContext().addTag(key, configValue)
      case Failure(_)           =>
    }
  }
}
