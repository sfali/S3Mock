package com.loyalty.testing.s3.repositories.collections

import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories._
import org.dizitart.no2._
import org.dizitart.no2.filters.Filters.{eq => feq, _}

class NotificationCollection(db: Nitrite) {

  import Document._

  private[repositories] val collection = db.getCollection("notification")
  if (!collection.hasIndex(NotificationNameField)) {
    collection.createIndex(NotificationNameField, IndexOptions.indexOptions(IndexType.NonUnique))
  }

  private[repositories] def createNotifications(notifications: List[Notification]): Int = {
    if (notifications.nonEmpty) {
      val bucketName = notifications.head.bucketName
      deleteNotifications(bucketName) // delete current notifications
      val documents =
        notifications.map {
          notification =>
            createDocument(BucketNameField, notification.bucketName)
              .put(NotificationNameField, notification.name)
              .put(NotificationTypeField, notification.notificationType.entryName)
              .put(OperationTypeField, notification.operationType.entryName)
              .put(DestinationTypeField, notification.destinationType.entryName)
              .put(DestinationNameField, notification.destinationName)
              .put(PrefixField, notification.prefix.orNull)
              .put(SuffixField, notification.suffix.orNull)
        }
      collection.insert(documents.toArray).getAffectedCount
    } else 0
  }

  def findNotifications(bucketName: String): List[Notification] = findByBucketName(bucketName).map(_.toNotification)

  def findNotification(bucketName: String, notificationName: String): Option[Notification] = {
    findByBucketNameAndNotificationName(bucketName, notificationName) match {
      case Nil => None
      case document :: Nil => Some(document.toNotification)
      case _ => throw new IllegalStateException(s"multiple bucket-notification pair found: $bucketName/$notificationName")
    }
  }

  private def deleteNotifications(bucketName: String) =
    collection.remove(bucketNameFilter(bucketName)).getAffectedCount

  private def findByBucketName(bucketName: String) =
    collection.find(bucketNameFilter(bucketName)).toScalaList

  private def findByBucketNameAndNotificationName(bucketName: String,
                                                  notificationName: String) =
    collection.find(and(bucketNameFilter(bucketName),
      feq(NotificationNameField, notificationName))).toScalaList

  private lazy val bucketNameFilter: String => Filter = bucketName => feq(BucketNameField, bucketName)
}

object NotificationCollection {
  def apply(db: Nitrite): NotificationCollection = new NotificationCollection(db)
}
