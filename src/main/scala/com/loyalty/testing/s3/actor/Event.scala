package com.loyalty.testing.s3.actor

import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}

import scala.concurrent.Future

sealed trait Event

case object NoSuchBucketExists extends Event

case object NoSuchKeyExists extends Event

case object InvalidAccess extends Event

case object NotificationsCreated extends Event

final case class BucketInfo(bucket: Bucket) extends Event

final case class NotificationsInfo(notifications: List[Notification]) extends Event

final case class ObjectInfo(objectKey: ObjectKey) extends Event

final case class ObjectContent(objectKey: ObjectKey, content: Source[ByteString, Future[IOResult]]) extends Event

final case class BucketAlreadyExists(bucket: Bucket) extends Event
