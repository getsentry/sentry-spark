package io.sentry.spark.listeners;

import org.apache.spark.sql.streaming.StreamingQueryListener;

import io.sentry.{SentryClient, SentryClientFactory};

import io.sentry.event.{Event, BreadcrumbBuilder, EventBuilder};

class SentrySparkQueryListener extends StreamingQueryListener {
  private val sentry: SentryClient = SentryClientFactory.sentryClient();
  private var name: String = "";

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent) {
    sentry.getContext().addTag("query_id", event.id.toString);

    name = event.name;

    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Query ${name} started")
          .withData("runId", event.runId.toString)
          .build()
      );
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent) {
    val progress = event.progress;

    name = progress.name;

    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Query ${name} progressed")
          .withData("runId", progress.runId.toString)
          .withData("timestamp", progress.timestamp)
          .withData("json", progress.json)
          .build()
      );
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent) {
    event.exception match {
      case Some(exception) => {
        val eventBuilder: EventBuilder = new EventBuilder()
          .withMessage(exception)
          .withLevel(Event.Level.ERROR)

        sentry.sendEvent(eventBuilder);
      }
      case None => {
        sentry
          .getContext()
          .recordBreadcrumb(
            new BreadcrumbBuilder()
              .setMessage(s"Query ${name} terminated")
              .withData("runId", event.runId.toString)
              .build()
          );
      }
    }
  }
}
