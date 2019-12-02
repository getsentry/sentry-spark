package io.sentry.spark.testUtil;

import io.sentry.context.ContextManager;
import io.sentry.context.SingletonContextManager;
import io.sentry.{Sentry, SentryClient, SentryClientFactory};
import io.sentry.event.Event;
import io.sentry.connection.{Connection, EventSendCallback};

import org.apache.spark.internal.Logging;

import org.scalatest._;

class MockConnection extends Connection {
  var hasSent = false;
  var lastEvent: Any = null;

  def resetMockConnection() {
    hasSent = false;
    lastEvent = null;
  }

  override def send(event: Event) {
    hasSent = true;
    lastEvent = event;
  }

  override def close() = ()

  override def addEventSendCallback(eventSendCallback: EventSendCallback) = ()
}

trait SetupSentry extends BeforeAndAfterEach { this: Suite =>
  val connection = new MockConnection();
  val contextManager = new SingletonContextManager();

  override def beforeEach() {
    connection.resetMockConnection();
    contextManager.clear();
    val sentryClient = new SentryClient(connection, contextManager);
    Sentry.setStoredClient(sentryClient);
    super.beforeEach();
  }
}

abstract class SentryBaseSpec
    extends FlatSpec
    with Matchers
    with PartialFunctionValues
    with Assertions
    with SetupSentry;
