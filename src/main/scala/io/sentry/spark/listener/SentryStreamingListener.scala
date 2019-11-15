package io.sentry.spark.listener;

import io.sentry.spark.util.Time;

import org.apache.spark.streaming.scheduler._;

import io.sentry.Sentry;
import io.sentry.event.{Event, EventBuilder, BreadcrumbBuilder};

class SentryStreamingListener extends StreamingListener {
  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted) {
    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage("Streaming started")
          .withData("time", Time.epochMilliToDateString(streamingStarted.time))
          .build()
      );
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    val info = receiverStarted.receiverInfo;

    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Receiver ${info.name} started on ${info.location}")
          .withData("streamId", info.streamId.toString)
          .withData("executorId", info.executorId.toString)
          .build()
      );
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    val info = receiverStopped.receiverInfo;

    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Receiver ${info.name} stopped on ${info.location}")
          .withData("streamId", info.streamId.toString)
          .withData("executorId", info.executorId.toString)
          .build()
      );
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    val info = receiverError.receiverInfo;

    val eventBuilder: EventBuilder = new EventBuilder()
      .withMessage(info.error)
      .withTag("name", info.name)
      .withTag("location", info.location)
      .withTag("streamId", info.streamId.toString)
      .withTag("executorId", info.executorId.toString)
      .withLevel(Event.Level.ERROR)

    Sentry.capture(eventBuilder);
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    val info = batchStarted.batchInfo;

    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Batch started with ${info.numRecords} records")
          .withData("submissionTime", Time.epochMilliToDateString(submissionTime))
          .build()
      );
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val info = batchCompleted.batchInfo;

    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Batch completed with ${info.numRecords} records")
          .withData("submissionTime", Time.epochMilliToDateString(submissionTime))
          .build()
      );
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted) {
    val info = batchCompleted.batchInfo;

    info.failureReason match {
      case Some(reason) => {
        val eventBuilder: EventBuilder = new EventBuilder()
          .withMessage(reason)
          .withTag("name", info.name)
          .withTag("id", info.id)
          .withExtra("description", info.description)
          .withLevel(Event.Level.ERROR)

        Sentry.capture(eventBuilder);
      }
      case None =>
    }
}
