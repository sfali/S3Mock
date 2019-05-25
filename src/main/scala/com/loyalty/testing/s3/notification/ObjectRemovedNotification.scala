package com.loyalty.testing.s3.notification

import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed trait ObjectRemovedNotification extends EnumEntry

object ObjectRemovedNotification
  extends Enum[ObjectRemovedNotification]
    with CirceEnum[ObjectRemovedNotification] {
  override def values: immutable.IndexedSeq[ObjectRemovedNotification] = findValues

  case object ObjectRemoveAll extends ObjectRemovedNotification

  case object Delete extends ObjectRemovedNotification

  case object DeleteMarkerCreated extends ObjectRemovedNotification

}


