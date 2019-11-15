package io.sentry.spark;

import scala.util.{Try, Success, Failure}

import org.apache.spark.SparkContext;

import io.sentry.Sentry;
import io.sentry.SentryClientFactory;
import io.sentry.event.UserBuilder;

object SentrySpark {
  def init() {
    Sentry.init();
  }

  def applyContext(sc: SparkContext) {
    this.setTags(sc);

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

  def setTags(sc: SparkContext) {
    Sentry
      .getContext()
      .setUser(
        new UserBuilder().setUsername(sc.sparkUser).build()
      );

    Sentry.getContext().addTag("version", sc.version);
    Sentry.getContext().addTag("app_name", sc.appName);
    Sentry.getContext().addTag("application_id", sc.applicationId);
    Sentry.getContext().addTag("master", sc.master);
  }
}
