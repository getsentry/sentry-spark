package io.sentry.spark.listener;

import org.apache.spark.sql.util.QueryExecutionListener;
import org.apache.spark.sql.execution.QueryExecution

import io.sentry.spark.util.Time;

import io.sentry.Sentry;
import io.sentry.event.interfaces.ExceptionInterface;
import io.sentry.event.{Event, BreadcrumbBuilder, EventBuilder};

class SentryQueryExecutionListener extends QueryExecutionListener {
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception) {
    val eventBuilder: EventBuilder = new EventBuilder()
      .withSdkIntegration("spark_scala")
      .withMessage(funcName)
      .withLevel(Event.Level.ERROR)
      .withSentryInterface(new ExceptionInterface(exception));

    Sentry.capture(eventBuilder)
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) {
    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Query ${funcName} executed succesfully")
          .build()
      );
  };
}
