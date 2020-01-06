package com.loyalty.testing.s3.repositories.model

import enumeratum.{CirceEnum, Enum, EnumEntry}


sealed trait ObjectStatus extends EnumEntry

object ObjectStatus extends Enum[ObjectStatus] with CirceEnum[ObjectStatus] {
  override def values: IndexedSeq[ObjectStatus] = findValues

  case object Active extends ObjectStatus

  case object DeleteMarker extends ObjectStatus

  case object Deleted extends ObjectStatus

  case object DeleteMarkerDeleted extends ObjectStatus
}
