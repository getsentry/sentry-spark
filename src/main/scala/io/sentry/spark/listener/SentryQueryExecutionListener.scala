package io.sentry.spark.listener;

import org.apache.spark.sql.util.QueryExecutionListener;
import org.apache.spark.sql.execution.QueryExecution

import io.sentry.Sentry;

import io.sentry.event.BreadcrumbBuilder;

class SentryQueryExecutionListener extends QueryExecutionListener {
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception) {
    Sentry.getContext().addExtra("query_action", funcName);

    Sentry.capture(exception)
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) {
    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Query ${funcName} executed succesfully")
          .withData("duration", durationNs.toString)
          .build()
      );
  };
}
