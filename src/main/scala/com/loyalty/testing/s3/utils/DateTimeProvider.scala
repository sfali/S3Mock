package com.loyalty.testing.s3.utils

import java.time.{OffsetDateTime, ZoneOffset, ZonedDateTime}

trait DateTimeProvider {
  def currentZonedDateTime: ZonedDateTime

  def currentOffsetDateTime: OffsetDateTime
}

object DateTimeProvider {
  def apply(): DateTimeProvider = new DefaultDateTimeProvider()
}

class DefaultDateTimeProvider extends DateTimeProvider {
  override def currentZonedDateTime: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC)

  override def currentOffsetDateTime: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
}

/**
  * Used in tests only
  */
class StaticDateTimeProvider extends DateTimeProvider {
  private var _currentDateTime: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC)
  private var _currentOffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)

  override def currentZonedDateTime: ZonedDateTime = _currentDateTime

  def currentZonedDateTime_=(current: ZonedDateTime): Unit = _currentDateTime = current

  override def currentOffsetDateTime: OffsetDateTime = _currentOffsetDateTime

  def currentOffsetDateTime_=(current: OffsetDateTime): Unit = _currentOffsetDateTime = current
}

object StaticDateTimeProvider {
  def apply(): StaticDateTimeProvider = new StaticDateTimeProvider()
}
