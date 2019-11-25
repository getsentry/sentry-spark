package io.sentry.spark.listener;

import org.apache.spark.sql.streaming.StreamingQueryListener;

import io.sentry.Sentry;
import io.sentry.event.{Event, BreadcrumbBuilder, EventBuilder};

class SentryStreamingQueryListener extends StreamingQueryListener {
  private var name: String = "";

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent) {
    Sentry.getContext().addTag("query_id", event.id.toString);

    name = event.name;

    Sentry
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

    Sentry
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
          .withTag("name", name)
          .withTag("runId", event.runId.toString)
          .withTag("id", event.id.toString)
          .withLevel(Event.Level.ERROR)

        Sentry.capture(eventBuilder);
      }
      case None => {
        Sentry
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
