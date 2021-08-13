package io.sentry.spark.testUtil;

import org.apache.spark.internal.Logging;

import org.scalatest._;
import io.sentry.{Sentry, Breadcrumb, SentryEvent, Scope, SentryOptions};
import scala.collection.mutable.ArrayBuffer;
import io.sentry.spark.util.SentryHelper

trait SetupSentry extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  val events: ArrayBuffer[SentryEvent] = ArrayBuffer();
  val breadcrumbs: ArrayBuffer[Breadcrumb] = ArrayBuffer();

  override def beforeAll() {
    SentryHelper.init((options: SentryOptions) => {
      options.setDsn("https://username@domain/path");

      options.setBeforeSend(new SentryOptions.BeforeSendCallback {
        override def execute(event: SentryEvent, hint: Any): SentryEvent = {
          println("[sentry] event being appended");
          events.append(event)
          event
        }
      });

      options.setBeforeBreadcrumb(new SentryOptions.BeforeBreadcrumbCallback {
        override def execute(breadcrumb: Breadcrumb, hint: Any): Breadcrumb = {
          breadcrumbs.append(breadcrumb);
          breadcrumb
        }
      });
    });

    super.beforeAll();
  }

  override def beforeEach() {
    events.clear;
    breadcrumbs.clear;
    SentryHelper.configureScope((scope: Scope) => {
      scope.clear();
    });
    super.beforeEach();
  }
}

abstract class SentryBaseSpec
    extends FlatSpec
    with Matchers
    with PartialFunctionValues
    with Assertions
    with SetupSentry;
