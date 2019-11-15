package io.sentry.spark.util;

import java.time.{Instant, ZoneId, ZonedDateTime}

object Time {
  def epochMilliToDateString(time: Long): String = {
    val instant = Instant.ofEpochMilli(time);
    val zonedDAteTimeUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
    zonedDAteTimeUtc.toString();
  }
}
