package com.loyalty.testing.s3.notification

case class NotificationData(bucketName: String,
                            key: String,
                            size: Long,
                            eTag: String,
                            operation: OperationType,
                            maybeVersionId: Option[String] = None)

@deprecated
case class NotificationMeta(configName: String,
                            notificationType: NotificationType,
                            operationType: OperationType,
                            destinationType: DestinationType,
                            destinationName: String)

object NotificationMeta {
  @deprecated
  def apply(notification: Notification): NotificationMeta =
    NotificationMeta(
      configName = notification.name,
      notificationType = notification.notificationType,
      operationType = notification.operationType,
      destinationType = notification.destinationType,
      destinationName = notification.destinationName
    )
}