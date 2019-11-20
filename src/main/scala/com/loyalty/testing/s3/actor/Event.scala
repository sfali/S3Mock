package com.loyalty.testing.s3.actor

import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}

sealed trait Event

case object NoSuchBucketExists extends Event

case object NoSuchKeyExists extends Event

case object InvalidAccess extends Event

case object NotificationsCreated extends Event

final case class BucketInfo(bucket: Bucket) extends Event

final case class NotificationsInfo(notifications: List[Notification]) extends Event

final case class ObjectInfo(objectKey: ObjectKey) extends Event

final case class BucketAlreadyExists(bucket: Bucket) extends Event
