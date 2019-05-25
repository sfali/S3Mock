package com.loyalty.testing.s3.notification

import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed trait ObjectCreatedNotification extends EnumEntry

object ObjectCreatedNotification
  extends Enum[ObjectCreatedNotification]
    with CirceEnum[ObjectCreatedNotification] {
  override def values: immutable.IndexedSeq[ObjectCreatedNotification] = findValues

  case object ObjectCreateAll extends ObjectCreatedNotification

  case object Put extends ObjectCreatedNotification

  case object Post extends ObjectCreatedNotification

  case object Copy extends ObjectCreatedNotification

  case object CompleteMultipartUpload extends ObjectCreatedNotification
}
