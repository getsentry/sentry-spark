package io.sentry.spark;

import org.apache.spark.SparkContext;

import io.sentry.Sentry;
import io.sentry.SentryClientFactory;
import io.sentry.event.UserBuilder;

object SentrySpark {
  def applyContext(sc: SparkContext) {
    Sentry.init();

    this.setTags(sc);

    val sparkConf = sc.getConf;

    val tags: List[(String, String)] = List(
      ("spark-submit.deployMode", "spark.submit.deployMode"),
      ("executor.id", "spark.executor.id"),
      ("driver.host", "spark.driver.host"),
      ("driver.port", "spark.driver.port")
    );

    tags.foreach(
      tag => Sentry.getContext().addTag(tag._1, sparkConf.get(tag._2))
    );
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
