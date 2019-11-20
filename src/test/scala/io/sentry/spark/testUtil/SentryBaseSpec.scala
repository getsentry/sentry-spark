package io.sentry.spark.testUtil;

import java.util.UUID;

import io.sentry.context.ContextManager;
import io.sentry.context.SingletonContextManager;
import io.sentry.{Sentry, SentryClient, SentryClientFactory};
import io.sentry.event.Event;
import io.sentry.connection.{Connection, EventSendCallback};

import org.scalatest._;

class MockConnection extends Connection {
  var hasSent = false;
  var event: Any = null;

  def resetMockConnection() {
    this.hasSent = false;
    this.event = null;
  }

  override def send(event: Event) {
    this.hasSent = true;
    this.event = event;
  }

  override def close() = ()

  override def addEventSendCallback(eventSendCallback: EventSendCallback) = ()
}

trait SetupSentry extends BeforeAndAfterEach { this: Suite =>
  val connection = new MockConnection();
  val contextManager = new SingletonContextManager();

  override def beforeEach() {
    this.contextManager.clear();
    val sentryClient = new SentryClient(this.connection, this.contextManager);
    Sentry.setStoredClient(sentryClient);
    super.beforeEach();
  }

  override def afterEach() {
    try {
      super.afterEach();
    } finally {
      this.connection.resetMockConnection();
      Sentry.getContext().clear()
    }
  }
}

abstract class SentryBaseSpec
    extends FlatSpec
    with Matchers
    with PartialFunctionValues
    with Assertions
    with SetupSentry;
