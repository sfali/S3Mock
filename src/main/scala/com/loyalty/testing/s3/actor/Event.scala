package com.loyalty.testing.s3.actor

import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.Bucket

sealed trait Event

case object NoSuchBucketExists extends Event

case object NotificationsCreated extends Event

final case class BucketInfo(bucket: Bucket) extends Event

final case class NotificationsInfo(notifications: List[Notification]) extends Event

final case class BucketAlreadyExists(bucket: Bucket) extends Event
