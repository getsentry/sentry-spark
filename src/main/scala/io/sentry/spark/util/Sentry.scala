package io.sentry.spark.util

import io.sentry.{Sentry, Scope, ScopeCallback, SentryOptions}

object SentryHelper {
  def init(callback: SentryOptions => Unit) {
    Sentry.init(new Sentry.OptionsConfiguration[SentryOptions] {
      def configure(options: SentryOptions) {
        callback(options)
      }
    })
  }

  def configureScope(callback: Scope => Unit) {
    Sentry.configureScope(new ScopeCallback {
      def run(scope: Scope) {
        callback(scope)
      }
    })
  }
}
