class SentrySparkQueryListener extends StreamingQueryListener with Logging {
  private val sentry: SentryClient = SentryClientFactory.sentryClient();


}