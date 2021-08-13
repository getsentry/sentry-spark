package io.sentry.spark;

import io.sentry.spark.util.SentryHelper

import scala.util.{Try, Success, Failure}

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.SparkContext;

import io.sentry.{Sentry, SentryOptions, Scope, ScopeCallback};
import io.sentry.protocol.User;

object SentrySpark {
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
    val tags: List[(String, String)] = List(
      ("spark-submit.deployMode", "spark.submit.deployMode"),
      ("executor.id", "spark.executor.id"),
      ("driver.host", "spark.driver.host"),
      ("driver.port", "spark.driver.port")
    );
    val sparkConf = sc.getConf;
    def getConf(value: String) = Try {
      sparkConf.get(value)
    }

    SentryHelper.configureScope((scope: Scope) => {
      // Set Spark User
      val user = new User();
      user.setUsername(sc.sparkUser);
      scope.setUser(user);

      // Set SparkContext tags
      scope.setTag("version", sc.version);
      scope.setTag("app_name", sc.appName);
      scope.setTag("application_id", sc.applicationId);
      scope.setTag("master", sc.master);

      for ((key, value) <- tags) getConf(value) match {
        case Success(configValue) => scope.setTag(key, configValue)
        case Failure(_)           =>
      }
    });
  }
}
