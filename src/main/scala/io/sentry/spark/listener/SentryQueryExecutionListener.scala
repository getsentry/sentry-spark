package io.sentry.spark.listener;

import org.apache.spark.sql.util.QueryExecutionListener;
import org.apache.spark.sql.execution.QueryExecution

import io.sentry.spark.util.Time;

import io.sentry.{Sentry, Breadcrumb, SentryEvent, SentryLevel};
import io.sentry.protocol.Message

class SentryQueryExecutionListener extends QueryExecutionListener {
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception) {
    val event = new SentryEvent();
    event.setThrowable(exception);
    event.setLevel(SentryLevel.ERROR);

    val message = new Message();
    message.setFormatted(funcName);
    event.setMessage(message);

    Sentry.captureEvent(event);
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) {
    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage(s"Query ${funcName} executed succesfully");
    breadcrumb.setData("duration", durationNs);
    Sentry.addBreadcrumb(breadcrumb);
  };
}
