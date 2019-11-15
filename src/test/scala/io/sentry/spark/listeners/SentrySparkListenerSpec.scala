package io.sentry.spark.listeners;

import scala.collection.JavaConversions._;

import org.scalatest._;

import org.apache.spark.scheduler._;
import org.apache.spark.{SparkContext, SparkConf};

import io.sentry.Sentry;
import io.sentry.event.{Breadcrumb};

class SentrySparkSpec extends FlatSpec with Matchers with PartialFunctionValues {
  val sparkListener = new SentrySparkListener();

  override def withFixture(test: NoArgTest) = {
    try {
      test();
    } finally {
      Sentry.getContext().clear();
    }
  }

  "SentrySparkListener.onApplicationStart" should "set tags" in {
    val AppName = "test-app-name";
    val AppId = "application-id-123";

    val mockAppStart =
      new SparkListenerApplicationStart(AppName, Some(AppId), 125L, "example-spark-user", None);

    sparkListener.onApplicationStart(mockAppStart);

    val tags = Sentry.getContext().getTags().toMap;
    tags.valueAt("app_name") should equal(AppName);
    tags.valueAt("application_id") should equal(AppId);
  }

  "SentrySparkListener.onApplicationStart" should "set breadcrumb" in {
    val AppName = "test-app-name";

    val mockAppStart =
      new SparkListenerApplicationStart(
        AppName,
        Some("app-id"),
        125L,
        "example-spark-user",
        None
      );
    sparkListener.onApplicationStart(mockAppStart);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(AppName);
    breadcrumb.getData() should contain key ("time")
  }

  "SentrySparkListener.onApplicationEnd" should "set breadcrumb" in {
    val mockAppEnd = new SparkListenerApplicationEnd(125L);
    sparkListener.onApplicationEnd(mockAppEnd);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include("ended");
    breadcrumb.getData() should contain key ("time")
  }

  "SentrySparkListener.onJobStart" should "set breadcrumb" in {
    val JobId = 12;

    val mockJobStart = new SparkListenerJobStart(JobId, 125L, Seq.empty);
    sparkListener.onJobStart(mockJobStart);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(JobId.toString);
    breadcrumb.getData() should contain key ("time")
  }

  "SentrySparkListener.onJobEnd" should "set breadcrumb with success" in {
    val JobId = 12;

    val mockJobEnd = new SparkListenerJobEnd(JobId, 125L, JobSucceeded);
    sparkListener.onJobEnd(mockJobEnd);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(JobId.toString);
    breadcrumb.getMessage() should include("Ended");
    assert(breadcrumb.getLevel() == (Breadcrumb.Level.INFO));
    breadcrumb.getData() should contain key ("time")
  }

  // Test does not work as JobFailed is defined as private[spark] case class
  // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/JobResult.scala#L32
  "SentrySparkListener.onJobEnd" should "set breadcrumb with failure" ignore {
    val JobId = 12;

    // val mockJobStart = new SparkListenerJobEnd(JobId, 125L, JobFailed);
    val mockJobEnd = new SparkListenerJobEnd(JobId, 125L, JobSucceeded);
    sparkListener.onJobEnd(mockJobEnd);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(JobId.toString);
    breadcrumb.getMessage() should include("Failed");
    assert(breadcrumb.getLevel() == (Breadcrumb.Level.ERROR));
    breadcrumb.getData() should contain key ("time")
  }

  "SentrySparkListener.onStageSubmitted" should "set breadcrumb" in {
    val StageId = 12;
    val StageName = "stage-name"
    val AttemptNumber = 0;

    val mockStageInfo =
      new StageInfo(StageId, AttemptNumber, StageName, 0, Seq.empty, Seq.empty, "details")

    val mockStageSubmitted = new SparkListenerStageSubmitted(mockStageInfo);
    sparkListener.onStageSubmitted(mockStageSubmitted);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(StageId.toString);

    val data = breadcrumb.getData().toMap;
    data.valueAt("name") should equal(StageName);
    data.valueAt("attemptNumber") should equal(AttemptNumber.toString);
  }

  "SentrySparkListener.onStageCompleted" should "set breadcrumb with success" in {
    val StageId = 12;
    val StageName = "stage-name"
    val AttemptNumber = 0;
    val FailureReason = "failed"

    val mockStageInfo =
      new StageInfo(StageId, AttemptNumber, StageName, 0, Seq.empty, Seq.empty, "details")

    val mockStageCompleted = new SparkListenerStageCompleted(mockStageInfo);
    sparkListener.onStageCompleted(mockStageCompleted);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(StageId.toString);
    breadcrumb.getMessage() should include("Completed");
    assert(breadcrumb.getLevel() == (Breadcrumb.Level.INFO));

    val data = breadcrumb.getData().toMap;
    data.valueAt("name") should equal(StageName);
    data.valueAt("attemptNumber") should equal(AttemptNumber.toString);
  }

  "SentrySparkListener.onStageCompleted" should "set breadcrumb with failure" in {
    val StageId = 12;
    val StageName = "stage-name"
    val AttemptNumber = 0;
    val FailureReason = "failed"

    val mockStageInfo =
      new StageInfo(StageId, AttemptNumber, StageName, 0, Seq.empty, Seq.empty, "details")

    mockStageInfo.failureReason = Some(FailureReason);

    val mockStageCompleted = new SparkListenerStageCompleted(mockStageInfo);
    sparkListener.onStageCompleted(mockStageCompleted);

    val breadcrumbs = Sentry.getContext().getBreadcrumbs();
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(StageId.toString);
    breadcrumb.getMessage() should include("Failed");
    assert(breadcrumb.getLevel() == (Breadcrumb.Level.ERROR));

    val data = breadcrumb.getData().toMap;
    data.valueAt("name") should equal(StageName);
    data.valueAt("attemptNumber") should equal(AttemptNumber.toString);
    data.valueAt("failureReason") should equal(FailureReason);
  }

  // TODO: Add tests for onTaskEnd
  "SentrySparkListener.onTaskEnd" should "set breadcrumb with failure" ignore {
  }
}
