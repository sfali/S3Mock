package com.loyalty.testing.s3.repositories

import org.dizitart.no2.filters.Filters.{eq => feq, _}
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.response.InvalidRequest
import org.dizitart.no2.{IndexOptions, IndexType, Nitrite, Document}

class NotificationCollection(db: Nitrite) {

  import Document._

  private[repositories] val collection = db.getCollection("notification")
  if (!collection.hasIndex(NotificationNameField)) {
    collection.createIndex(NotificationNameField, IndexOptions.indexOptions(IndexType.Unique))
  }

  def createNotification(notification: Notification): Notification = {
    val bucketName = notification.bucketName
    val notificationName = notification.name
    findByBucketNameAndNotificationName(bucketName, notificationName) match {
      case Nil =>
        val document =
          createDocument(BucketNameField, bucketName)
            .put(NotificationNameField, notificationName)
            .put(NotificationTypeField, notification.notificationType.entryName)
            .put(OperationTypeField, notification.operationType.entryName)
            .put(DestinationTypeField, notification.destinationType.entryName)
            .put(DestinationNameField, notification.destinationName)
            .put(PrefixField, notification.prefix.orNull)
            .put(SuffixField, notification.suffix.orNull)
        collection.insert(document)
        notification
      case document :: Nil =>
        // should never happen
        throw InvalidRequest(bucketName, s"Notification $notificationName already exists with document id ${document.getId}")
      case _ => throw new IllegalStateException(s"multiple bucket-notification pair found: $bucketName/$notificationName")
    }
  }

  def findNotification(bucketName: String, notificationName: String): Option[Notification] = {
    findByBucketNameAndNotificationName(bucketName, notificationName) match {
      case Nil => None
      case document :: Nil => Some(document.toNotification)
      case _ => throw new IllegalStateException(s"multiple bucket-notification pair found: $bucketName/$notificationName")
    }
  }

  private def findByBucketNameAndNotificationName(bucketName: String,
                                                  notificationName: String) =
    collection.find(and(feq(BucketNameField, bucketName),
      feq(NotificationNameField, notificationName))).toScalaList
}

object NotificationCollection {
  def apply(db: Nitrite): NotificationCollection = new NotificationCollection(db)
}
