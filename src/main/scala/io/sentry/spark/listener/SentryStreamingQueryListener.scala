package io.sentry.spark.listener;

import org.apache.spark.sql.streaming.StreamingQueryListener;

import io.sentry.{Sentry, Breadcrumb, SentryLevel, SentryEvent, Scope, ScopeCallback};
import io.sentry.protocol.Message

import io.sentry.spark.util.SentryHelper

class SentryStreamingQueryListener extends StreamingQueryListener {
  private var name: String = "";

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent) {
    name = event.name;
    SentryHelper.configureScope((scope: Scope) => {
      scope.setTag("query_id", event.id.toString);

      val breadcrumb = new Breadcrumb();
      breadcrumb.setData("runId", event.runId);
      breadcrumb.setMessage(s"Query ${name} started");
      scope.addBreadcrumb(breadcrumb);
    });
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent) {
    val progress = event.progress;
    name = progress.name;

    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage(s"Query ${name} progressed");
    breadcrumb.setData("runId", progress.runId);
    breadcrumb.setData("json", progress.json);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent) {
    event.exception match {
      case Some(exception) => {
        val sentryEvent = new SentryEvent();
        sentryEvent.setLevel(SentryLevel.ERROR);
        sentryEvent.setTag("name", name);
        sentryEvent.setTag("runId", event.runId.toString);
        sentryEvent.setTag("id", event.id.toString);

        val message = new Message();
        message.setFormatted(exception);
        sentryEvent.setMessage(message);

        Sentry.captureEvent(sentryEvent);
      }
      case None => {
        val breadcrumb = new Breadcrumb();
        breadcrumb.setMessage(s"Query ${name} terminated");
        breadcrumb.setData("runId", event.runId);
        Sentry.addBreadcrumb(breadcrumb);
      }
    }
  }
}
