package com.loyalty.testing.s3.notification

import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed trait DestinationType extends EnumEntry

object DestinationType extends Enum[DestinationType] with CirceEnum[DestinationType] {
  override def values: immutable.IndexedSeq[DestinationType] = findValues

  case object Sqs extends DestinationType

  case object Sns extends DestinationType

  case object Cloud extends DestinationType

}
