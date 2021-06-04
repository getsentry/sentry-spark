package io.sentry.spark.listener;

import io.sentry.spark.util.Time;

import org.apache.spark.{
  TaskFailedReason,
  TaskEndReason,
  ExceptionFailure,
  ExecutorLostFailure,
  FetchFailed,
  TaskCommitDenied
};

import org.apache.spark.scheduler._;

import io.sentry.{Sentry, Breadcrumb, SentryLevel, SentryEvent};
import io.sentry.protocol.Message;

class SentrySparkListener extends SparkListener {
  lazy val BreadcrumbCategory = "spark";

  override def onApplicationStart(
    applicationStart: SparkListenerApplicationStart
  ) {
    Sentry.configureScope((scope) => {
      scope.setTag("app_name", applicationStart.appName);
      applicationStart.appId match {
        case Some(id) => scope.setTag("application_id", id)
        case None     =>
      };
      applicationStart.appAttemptId match {
        case Some(id) => scope.setTag("app_attempt_id", id)
        case None     =>
      }

      val breadcrumb = new Breadcrumb();
      breadcrumb.setMessage(s"Application ${applicationStart.appName} started");
      breadcrumb.setCategory(BreadcrumbCategory);
      scope.addBreadcrumb(breadcrumb);
    });
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage("Application ended");
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val breadcrumb = new Breadcrumb();
    breadcrumb.setMessage(s"Job ${jobStart.jobId} Started");
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val dataTuple: (SentryLevel, String) = jobEnd.jobResult match {
      case JobSucceeded => (SentryLevel.INFO, s"Job ${jobEnd.jobId} Ended")
      case _            => (SentryLevel.ERROR, s"Job ${jobEnd.jobId} Failed")
    }

    val breadcrumb = new Breadcrumb();
    breadcrumb.setLevel(dataTuple._1);
    breadcrumb.setMessage(dataTuple._2);
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stageInfo = stageSubmitted.stageInfo

    val breadcrumb = new Breadcrumb();
    print(s"Adding name ${stageInfo.name} ${stageInfo.attemptNumber}");
    breadcrumb.setMessage(s"Stage ${stageInfo.stageId} Submitted");
    breadcrumb.setData("name", stageInfo.name);
    breadcrumb.setData("attempt_number", stageInfo.attemptNumber);
    breadcrumb.setCategory(BreadcrumbCategory);
    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stageInfo = stageCompleted.stageInfo;

    val breadcrumb = new Breadcrumb();
    breadcrumb.setData("name", stageInfo.name);
    breadcrumb.setData("attempt_number", stageInfo.attemptNumber);
    breadcrumb.setCategory(BreadcrumbCategory);

    stageInfo.failureReason match {
      case Some(reason) => {
        breadcrumb.setLevel(SentryLevel.ERROR);
        breadcrumb.setMessage(s"Stage ${stageInfo.stageId} Failed");
        breadcrumb.setData("failure_reason", reason);
      }
      case None => {
        breadcrumb.setLevel(SentryLevel.INFO);
        breadcrumb.setMessage(s"Stage ${stageInfo.stageId} Completed");
      }
    }

    Sentry.addBreadcrumb(breadcrumb);
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val reason: TaskEndReason = taskEnd.reason;
    reason match {
      case _: TaskFailedReason =>
        TaskEndProcessor.createEventFromReason(reason.asInstanceOf[TaskFailedReason])
      case _ =>
    }
  }
}

object TaskEndProcessor {
  def createEventFromReason(reason: TaskFailedReason) {
    val event = new SentryEvent();

    val message = new Message();
    message.setFormatted(reason.toErrorString);
    event.setMessage(message);

    val enhancedEvent = reason match {
      case _: ExceptionFailure =>
        this.enhanceEvent(event, reason.asInstanceOf[ExceptionFailure])
      case _: ExecutorLostFailure =>
        this.enhanceEvent(event, reason.asInstanceOf[ExecutorLostFailure])
      case _: FetchFailed => this.enhanceEvent(event, reason.asInstanceOf[FetchFailed])
      case _: TaskCommitDenied =>
        this.enhanceEvent(event, reason.asInstanceOf[TaskCommitDenied])
      case _ => event
    }

    Sentry.captureEvent(enhancedEvent);
  }

  private def enhanceEvent(event: SentryEvent, reason: ExceptionFailure): SentryEvent = {
    event.setLevel(SentryLevel.ERROR);
    event.setTag("className", reason.className);
    reason.exception match {
      case Some(exception) => {
        event.setThrowable(exception);
      }
      case None => {
        val throwable: Throwable = new Throwable(reason.description);
        throwable.setStackTrace(reason.stackTrace);
        event.setThrowable(throwable);
      }
    }
    event
  }

  private def enhanceEvent(event: SentryEvent, reason: ExecutorLostFailure): SentryEvent = {
    event.setLevel(SentryLevel.ERROR);
    event.setTag("execId", reason.execId);
    event.setExtra("exitCausedByApp", reason.exitCausedByApp);
    event
  }

  private def enhanceEvent(event: SentryEvent, reason: FetchFailed): SentryEvent = {
    event.setLevel(SentryLevel.WARNING);
    event.setTag("mapId", reason.mapId.toString);
    event.setTag("reduceId", reason.reduceId.toString);
    event.setTag("shuffleId", reason.shuffleId.toString);
    event
  }

  private def enhanceEvent(event: SentryEvent, reason: TaskCommitDenied): SentryEvent = {
    event.setLevel(SentryLevel.WARNING);
    event.setTag("attemptNumber", reason.attemptNumber.toString);
    event.setTag("jobID", reason.jobID.toString);
    event.setTag("partitionID", reason.partitionID.toString);
    event
  }
}
