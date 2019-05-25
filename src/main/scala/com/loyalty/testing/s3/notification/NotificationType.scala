package com.loyalty.testing.s3.notification

import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed trait NotificationType extends EnumEntry

object NotificationType extends Enum[NotificationType] with CirceEnum[NotificationType] {
  override def values: immutable.IndexedSeq[NotificationType] = findValues

  case object ObjectCreateAll extends NotificationType
}
