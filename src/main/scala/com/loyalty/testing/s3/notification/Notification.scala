package com.loyalty.testing.s3.notification

import java.util.UUID

case class Notification(name: String = UUID.randomUUID().toString,
                        notificationType: NotificationType = NotificationType.ObjectCreateAll,
                        destinationType: DestinationType = DestinationType.Sqs,
                        destinationName: String,
                        bucketName: String,
                        prefix: Option[String] = None,
                        suffix: Option[String] = None)
