package com.loyalty.testing.s3.notification

import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class NotificationType(operations: OperationType *) extends EnumEntry

object NotificationType extends Enum[NotificationType] with CirceEnum[NotificationType] {
  override def values: immutable.IndexedSeq[NotificationType] = findValues

  import OperationType._

  case object ObjectCreated extends NotificationType(*, Put, Post, Copy, CompleteMultipartUpload)

  case object ObjectRemoved extends NotificationType(*, Delete, DeleteMarkerCreated)
}
