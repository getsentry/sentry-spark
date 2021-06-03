package io.sentry.spark.testUtil;

import org.apache.spark.internal.Logging;

import org.scalatest._;
import io.sentry.{Sentry, SentryEvent};
import scala.collection.mutable.ArrayBuffer;

trait SetupSentry extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  val events: ArrayBuffer[SentryEvent] = ArrayBuffer();

  override def beforeAll() {
    Sentry.init((options) => {
      options.setDsn("https://username@domain/path");

      options.setBeforeSend((event, hint) => {
        events :+ event;
        null
      });
    });
    super.beforeAll();
  }

  override def beforeEach() {
    events.clear;
    Sentry.configureScope((scope) => {
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
