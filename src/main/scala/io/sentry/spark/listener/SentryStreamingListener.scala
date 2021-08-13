package io.sentry.spark.listener;

import io.sentry.spark.util.Time;

import org.apache.spark.streaming.scheduler._;
import io.sentry.{Sentry, Breadcrumb};
import io.sentry.SentryEvent
import io.sentry.SentryLevel
import io.sentry.protocol.Message

class SentryStreamingListener extends StreamingListener {
  lazy val BreadcrumbCategory = "spark-streaming";

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted) {
    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage("Streaming started");
    breadcrumb.setData("time", Time.epochMilliToDateString(streamingStarted.time));
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    val info = receiverStarted.receiverInfo;

    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage(s"Receiver ${info.name} started on ${info.location}");
    breadcrumb.setData("streamId", info.streamId.toString);
    breadcrumb.setData("executorId", info.executorId.toString);
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    val info = receiverStopped.receiverInfo;

    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage(s"Receiver ${info.name} stopped on ${info.location}");
    breadcrumb.setData("streamId", info.streamId.toString);
    breadcrumb.setData("executorId", info.executorId.toString);
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    val info = receiverError.receiverInfo;

    val event = new SentryEvent();
    event.setLevel(SentryLevel.ERROR);
    event.setTag("name", info.name);
    event.setTag("location", info.location);
    event.setTag("streamId", info.streamId.toString);
    event.setTag("executorId", info.executorId);

    val message = new Message();
    message.setFormatted(info.lastError);
    event.setMessage(message);

    Sentry.captureEvent(event);
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    val info = batchStarted.batchInfo;

    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage(s"Batch started with ${info.numRecords} records");
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val info = batchCompleted.batchInfo;

    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage(s"Batch completed with ${info.numRecords} records");
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onOutputOperationCompleted(
    outputOperationCompleted: StreamingListenerOutputOperationCompleted
  ) {
    val info = outputOperationCompleted.outputOperationInfo;

    info.failureReason match {
      case Some(reason) => {
        val event = new SentryEvent();
        event.setTag("name", info.name);
        event.setTag("id", info.id.toString);
        event.setExtra("description", info.description);
        event.setLevel(SentryLevel.ERROR);

        val message = new Message();
        message.setFormatted(reason);
        event.setMessage(message);

        Sentry.captureEvent(event);
      }
      case None =>
    }
  }
}
