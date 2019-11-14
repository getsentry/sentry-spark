package io.sentry.spark.listeners;

import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.spark.{
  TaskFailedReason,
  TaskEndReason,
  ExceptionFailure,
  ExecutorLostFailure,
  FetchFailed,
  TaskCommitDenied,
  TaskKilled,
  Resubmitted
};

import org.apache.spark.scheduler._;
import org.apache.spark.internal.Logging;

import io.sentry.{Sentry, SentryClient, SentryClientFactory};
import io.sentry.event.{Event, Breadcrumb, BreadcrumbBuilder};

class SentrySparkListener extends SparkListener with Logging {
  private val sentry: SentryClient = SentryClientFactory.sentryClient();

  override def onApplicationStart(
    applicationStart: SparkListenerApplicationStart
  ) {
    sentry.getContext().addTag("app_name", applicationStart.appName);
    applicationStart.appId match {
      case Some(id) => sentry.getContext().addTag("application_id", id)
      case None     =>
    };

    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Application ${applicationStart.appName} started")
          .withData("time", epochMilliToDateString(applicationStart.time))
          .build()
      );
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage("Application ended")
          .withData("time", epochMilliToDateString(applicationEnd.time))
          .build()
      );
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Job ${jobStart.jobId} Started")
          .withData("time", epochMilliToDateString(jobStart.time))
          .build()
      );
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val dataTuple: (Breadcrumb.Level, String) = jobEnd.jobResult match {
      case JobSucceeded => (Breadcrumb.Level.INFO, s"Job ${jobEnd.jobId} Ended")
      case _            => (Breadcrumb.Level.ERROR, s"Job ${jobEnd.jobId} Failed")
    }

    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setLevel(dataTuple._1)
          .setMessage(dataTuple._2)
          .withData("time", epochMilliToDateString(jobEnd.time))
          .build()
      );
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stageInfo = stageSubmitted.stageInfo

    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Stage ${stageInfo.stageId} Submitted")
          .withData("name", stageInfo.name)
          .withData("attemptNumber", stageInfo.attemptNumber.toString)
          .build()
      );
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stageInfo = stageCompleted.stageInfo

    val dataTuple: (Breadcrumb.Level, String) = stageInfo.failureReason match {
      case Some(reason) => (Breadcrumb.Level.ERROR, s"Stage ${stageInfo.stageId} Failed")
      case None         => (Breadcrumb.Level.INFO, s"Stage ${stageInfo.stageId} Completed")
    }

    sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setLevel(dataTuple._1)
          .setMessage(dataTuple._2)
          .withData("name", stageInfo.name)
          .withData("attemptNumber", stageInfo.attemptNumber.toString)
          .build()
      );
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val reason: TaskEndReason = taskEnd.reason;
    reason match {
      case _: TaskFailedReason =>
        TaskEndParser.parseTaskEndReason(reason.asInstanceOf[TaskFailedReason])
      case _ =>
    }
  }

  private def epochMilliToDateString(time: Long): String = {
    val instant = Instant.ofEpochMilli(time);
    val zonedDAteTimeUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
    zonedDAteTimeUtc.toString();
  }

  object TaskEndParser {
    def parseTaskEndReason(reason: TaskFailedReason) {
      reason match {
        case _: ExceptionFailure =>
          this.captureExceptionFailure(reason.asInstanceOf[ExceptionFailure])
        case _: ExecutorLostFailure =>
          this.captureExecutorLostFailure(reason.asInstanceOf[ExecutorLostFailure])
        case _: FetchFailed => this.captureFetchFailed(reason.asInstanceOf[FetchFailed])
        case _: TaskCommitDenied =>
          this.captureTaskCommitDenied(reason.asInstanceOf[TaskCommitDenied])
        case _ => this.captureErrorString(reason)
      }
    }

    private def captureExceptionFailure(reason: ExceptionFailure) {
      sentry.getContext().addTag("className", reason.className);
      sentry.getContext().addExtra("description", reason.description);

      reason.exception match {
        case Some(exception) => sentry.sendException(exception)
        case None => {
          val throwable: Throwable = new Throwable(reason.description);
          throwable.setStackTrace(reason.stackTrace)
          sentry.sendException(throwable)
        }
      }
    }

    private def captureExecutorLostFailure(reason: ExecutorLostFailure) {
      sentry.getContext().addTag("execId", reason.execId.toString);

      sentry.sendMessage(reason.toErrorString)
    }

    private def captureFetchFailed(reason: FetchFailed) {
      sentry.getContext().addTag("mapId", reason.mapId.toString);
      sentry.getContext().addTag("reduceId", reason.reduceId.toString);
      sentry.getContext().addTag("shuffleId", reason.shuffleId.toString);

      sentry.sendMessage(reason.toErrorString)
    }

    private def captureTaskCommitDenied(reason: TaskCommitDenied) {
      sentry.getContext().addTag("attemptNumber", reason.attemptNumber.toString);
      sentry.getContext().addTag("jobID", reason.jobID.toString);
      sentry.getContext().addTag("partitionID", reason.partitionID.toString);

      sentry.sendMessage(reason.toErrorString)
    }

    private def captureErrorString(reason: TaskFailedReason) {
      sentry.sendMessage(reason.toErrorString)
    }
  }
}
