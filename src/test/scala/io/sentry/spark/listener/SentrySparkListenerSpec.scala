package io.sentry.spark.listener;

import scala.collection.JavaConverters._;

import org.scalatest._;

import org.apache.spark.scheduler._;
import org.apache.spark.{SparkContext, SparkConf, SparkException};
import org.apache.spark.sql.SparkSession;

import io.sentry.{Sentry, SentryClient};

import io.sentry.spark.testUtil.SentryBaseSpec;
import io.sentry.SentryLevel

class SentrySparkListenerSpec extends SentryBaseSpec {
  val sparkListener = new SentrySparkListener();

  "SentrySparkListener.onApplicationStart" should "set tags" in {
    val AppName = "test-app-name";
    val AppId = "application-id-123";

    val mockAppStart =
      new SparkListenerApplicationStart(AppName, Some(AppId), 125L, "example-spark-user", None);

    sparkListener.onApplicationStart(mockAppStart);

    Sentry.configureScope((scope) => {
      val tags = scope.getTags().asScala;
      tags.valueAt("app_name") should equal(AppName);
      tags.valueAt("application_id") should equal(AppId);
    });
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

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(AppName);
  }

  "SentrySparkListener.onApplicationEnd" should "set breadcrumb" in {
    val mockAppEnd = new SparkListenerApplicationEnd(125L);
    sparkListener.onApplicationEnd(mockAppEnd);

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include("ended");
    this.breadcrumbs
  }

  "SentrySparkListener.onJobStart" should "set breadcrumb" in {
    val JobId = 12;

    val mockJobStart = new SparkListenerJobStart(JobId, 125L, Seq.empty);
    sparkListener.onJobStart(mockJobStart);

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(JobId.toString);
    this.breadcrumbs
  }

  "SentrySparkListener.onJobEnd" should "set breadcrumb with success" in {
    val JobId = 12;

    val mockJobEnd = new SparkListenerJobEnd(JobId, 125L, JobSucceeded);
    sparkListener.onJobEnd(mockJobEnd);

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(JobId.toString);
    breadcrumb.getMessage() should include("Ended");
    assert(breadcrumb.getLevel() == (SentryLevel.INFO));
    this.breadcrumbs
  }

  // Test does not work as JobFailed is defined as private[spark] case class
  // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/JobResult.scala#L32
  "SentrySparkListener.onJobEnd" should "set breadcrumb with failure" ignore {
    val JobId = 12;

    // val mockJobStart = new SparkListenerJobEnd(JobId, 125L, JobFailed);
    val mockJobEnd = new SparkListenerJobEnd(JobId, 125L, JobSucceeded);
    sparkListener.onJobEnd(mockJobEnd);

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(JobId.toString);
    breadcrumb.getMessage() should include("Failed");
    assert(breadcrumb.getLevel() == (SentryLevel.ERROR));
    this.breadcrumbs
  }

  "SentrySparkListener.onStageSubmitted" should "set breadcrumb" in {
    val StageId = 12;
    val StageName = "stage-name"
    val AttemptNumber = 0;

    val mockStageInfo =
      new StageInfo(StageId, AttemptNumber, StageName, 0, Seq.empty, Seq.empty, "details")

    val mockStageSubmitted = new SparkListenerStageSubmitted(mockStageInfo);
    sparkListener.onStageSubmitted(mockStageSubmitted);

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(StageId.toString);

    val data = breadcrumb.getData().asScala;
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

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(StageId.toString);
    breadcrumb.getMessage() should include("Completed");
    assert(breadcrumb.getLevel() == (SentryLevel.INFO));

    val data = breadcrumb.getData().asScala;
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

    val breadcrumbs = this.breadcrumbs;
    breadcrumbs should have length 1;

    val breadcrumb = breadcrumbs(0);
    breadcrumb.getMessage() should include(StageId.toString);
    breadcrumb.getMessage() should include("Failed");
    assert(breadcrumb.getLevel() == (SentryLevel.ERROR));

    val data = breadcrumb.getData().asScala;
    data.valueAt("name") should equal(StageName);
    data.valueAt("attemptNumber") should equal(AttemptNumber.toString);
    data.valueAt("failureReason") should equal(FailureReason);
  }

  // TODO: Add tests for onTaskEnd
  "SentrySparkListener.onTaskEnd" should "send to Sentry" ignore {}

  "SentrySparkListener" should "capture an error" in {
    assert(this.events.isEmpty);

    assertThrows[SparkException] {
      val spark = SparkSession.builder
        .appName("Simple Application")
        .master("local")
        .config("spark.ui.enabled", "false")
        .config("spark.extraListeners", "io.sentry.spark.listener.SentrySparkListener")
        .getOrCreate()

      val testFile = getClass.getResource("/test.txt").toString;
      val logData = spark.read.textFile(testFile).cache()

      val numAs = logData
        .filter(line => {
          throw new IllegalStateException("Exception thrown");
          line.contains("a")
        })
        .count()
      spark.stop()
    };

    Thread.sleep(1000)
    assert(this.events.length === 1);
  }
}
