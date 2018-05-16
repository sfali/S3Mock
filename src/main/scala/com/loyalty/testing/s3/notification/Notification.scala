package com.loyalty.testing.s3.notification

import java.util.UUID

import com.loyalty.testing.s3.notification.DestinationType.DestinationType
import com.loyalty.testing.s3.notification.NotificationType.NotificationType
import io.circe.generic.semiauto._
import io.circe._


case class Notification(name: String = UUID.randomUUID().toString,
                        notificationType: NotificationType = NotificationType.ObjectCreateAll,
                        destinationType: DestinationType = DestinationType.Sqs,
                        destinationName: String,
                        bucketName: String,
                        prefix: Option[String] = None,
                        suffix: Option[String] = None)

object Notification {
  implicit val NotificationDecoder: Decoder[Notification] = deriveDecoder[Notification]
  implicit val NotificationEncoder: Encoder[Notification] = deriveEncoder[Notification]
}
