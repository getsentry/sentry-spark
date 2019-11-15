package io.sentry.spark.listener;

import io.sentry.spark.util.Time;

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
import io.sentry.event.{Event, Breadcrumb, BreadcrumbBuilder, UserBuilder};

class SentrySparkListener extends SparkListener with Logging {
  override def onApplicationStart(
    applicationStart: SparkListenerApplicationStart
  ) {
    Sentry.getContext().addTag("app_name", applicationStart.appName);
    applicationStart.appId match {
      case Some(id) => Sentry.getContext().addTag("application_id", id)
      case None     =>
    };

    Sentry
      .getContext()
      .setUser(
        new UserBuilder().setUsername(applicationStart.sparkUser).build()
      );

    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Application ${applicationStart.appName} started")
          .withData("time", Time.epochMilliToDateString(applicationStart.time))
          .build()
      );
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage("Application ended")
          .withData("time", Time.epochMilliToDateString(applicationEnd.time))
          .build()
      );
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setMessage(s"Job ${jobStart.jobId} Started")
          .withData("time", Time.epochMilliToDateString(jobStart.time))
          .build()
      );
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val dataTuple: (Breadcrumb.Level, String) = jobEnd.jobResult match {
      case JobSucceeded => (Breadcrumb.Level.INFO, s"Job ${jobEnd.jobId} Ended")
      case _            => (Breadcrumb.Level.ERROR, s"Job ${jobEnd.jobId} Failed")
    }

    Sentry
      .getContext()
      .recordBreadcrumb(
        new BreadcrumbBuilder()
          .setLevel(dataTuple._1)
          .setMessage(dataTuple._2)
          .withData("time", Time.epochMilliToDateString(jobEnd.time))
          .build()
      );
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stageInfo = stageSubmitted.stageInfo

    Sentry
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

    val breadcrumbBuilderWithoutMessage = new BreadcrumbBuilder()
      .withData("name", stageInfo.name)
      .withData("attemptNumber", stageInfo.attemptNumber.toString);

    val breadcrumbBuilder: BreadcrumbBuilder = stageInfo.failureReason match {
      case Some(reason) =>
        breadcrumbBuilderWithoutMessage
          .setLevel(Breadcrumb.Level.ERROR)
          .setMessage(s"Stage ${stageInfo.stageId} Failed")
          .withData("failureReason", reason)
      case None =>
        breadcrumbBuilderWithoutMessage
          .setLevel(Breadcrumb.Level.INFO)
          .setMessage(s"Stage ${stageInfo.stageId} Completed")
    }

    Sentry
      .getContext()
      .recordBreadcrumb(
        breadcrumbBuilder
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
    Sentry.getContext().addTag("className", reason.className);
    Sentry.getContext().addExtra("description", reason.description);

    reason.exception match {
      case Some(exception) => Sentry.capture(exception)
      case None => {
        val throwable: Throwable = new Throwable(reason.description);
        throwable.setStackTrace(reason.stackTrace)
        Sentry.capture(throwable)
      }
    }
  }

  private def captureExecutorLostFailure(reason: ExecutorLostFailure) {
    Sentry.getContext().addTag("execId", reason.execId.toString);

    Sentry.capture(reason.toErrorString)
  }

  private def captureFetchFailed(reason: FetchFailed) {
    Sentry.getContext().addTag("mapId", reason.mapId.toString);
    Sentry.getContext().addTag("reduceId", reason.reduceId.toString);
    Sentry.getContext().addTag("shuffleId", reason.shuffleId.toString);

    Sentry.capture(reason.toErrorString)
  }

  private def captureTaskCommitDenied(reason: TaskCommitDenied) {
    Sentry.getContext().addTag("attemptNumber", reason.attemptNumber.toString);
    Sentry.getContext().addTag("jobID", reason.jobID.toString);
    Sentry.getContext().addTag("partitionID", reason.partitionID.toString);

    Sentry.capture(reason.toErrorString)
  }

  private def captureErrorString(reason: TaskFailedReason) {
    Sentry.capture(reason.toErrorString)
  }
}
