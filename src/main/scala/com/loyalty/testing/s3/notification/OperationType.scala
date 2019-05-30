package com.loyalty.testing.s3.notification

import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed trait OperationType extends EnumEntry

object OperationType extends Enum[OperationType] with CirceEnum[OperationType] {
  override def values: immutable.IndexedSeq[OperationType] = findValues

  case object * extends OperationType

  case object Put extends OperationType

  case object Post extends OperationType

  case object Copy extends OperationType

  case object CompleteMultipartUpload extends OperationType

  case object Delete extends OperationType

  case object DeleteMarkerCreated extends OperationType
}
