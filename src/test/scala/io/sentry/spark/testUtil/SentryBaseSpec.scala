package io.sentry.spark.testUtil;

import org.apache.spark.internal.Logging;

import org.scalatest._;
import io.sentry.{Sentry, Breadcrumb, SentryEvent, Scope, SentryOptions};
import scala.collection.mutable.ArrayBuffer;

trait SetupSentry extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  val events: ArrayBuffer[SentryEvent] = ArrayBuffer();
  val breadcrumbs: ArrayBuffer[Breadcrumb] = ArrayBuffer();

  override def beforeAll() {
    Sentry.init((options: SentryOptions) => {
      options.setDsn("https://username@domain/path");

      options.setBeforeSend((event: SentryEvent, hint: Any) => {
        events.append(event);
        null
      });

      options.setBeforeBreadcrumb((breadcrumb: Breadcrumb, hint: Any) => {
        breadcrumbs.append(breadcrumb);
        breadcrumb
      });
    });
    super.beforeAll();
  }

  override def afterEach() {
    events.clear;
    breadcrumbs.clear;
    Sentry.configureScope((scope: Scope) => {
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
