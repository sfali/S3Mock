package com.loyalty.testing.s3.notification

import java.util.UUID

import com.loyalty.testing.s3.notification.DestinationType.DestinationType
import com.loyalty.testing.s3.notification.NotificationType.NotificationType

case class Notification(name: String = UUID.randomUUID().toString,
                        notificationType: NotificationType = NotificationType.ObjectCreateAll,
                        destinationType: DestinationType = DestinationType.Sqs,
                        destinationName: String,
                        prefix: Option[String] = None,
                        suffix: Option[String] = None)
