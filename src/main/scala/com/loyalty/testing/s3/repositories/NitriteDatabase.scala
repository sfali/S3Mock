package com.loyalty.testing.s3.repositories

import java.util.UUID

import akka.Done
import akka.actor.typed.ActorSystem
import com.loyalty.testing.s3.DBSettings
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.collections.{BucketCollection, CreateResponse, NotificationCollection, ObjectCollection}
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.VersioningConfiguration
import com.loyalty.testing.s3.response.{ObjectMeta, PutObjectResult}
import com.loyalty.testing.s3.utils.DateTimeProvider
import org.dizitart.no2.Nitrite

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class NitriteDatabase(dbSettings: DBSettings)(implicit system: ActorSystem[Nothing],
                                              dateTimeProvider: DateTimeProvider) {

  import system.executionContext

  private val db: Nitrite = {
    val _db = Nitrite
      .builder()
      .compressed()
      .filePath(dbSettings.filePath)

    dbSettings.userName match {
      case Some(userName) => _db.openOrCreate(userName, dbSettings.password.getOrElse(userName))
      case None => _db.openOrCreate()
    }
  }

  private[repositories] val bucketCollection = BucketCollection(db)
  private[repositories] val notificationCollection = NotificationCollection(db)
  private[repositories] val objectCollection = ObjectCollection(db)

  def getBucket(id: UUID): Future[Bucket] =
    Try(bucketCollection.findBucket(id)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(bucket) => Future.successful(bucket)
    }

  def createBucket(bucket: Bucket): Future[Bucket] =
    Try(bucketCollection.createBucket(bucket)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(bucket) => Future.successful(bucket)
    }

  def setBucketVersioning(bucketId: UUID, versioningConfiguration: VersioningConfiguration): Future[Bucket] =
    Try(bucketCollection.setBucketVersioning(bucketId, versioningConfiguration.bucketVersioning)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(bucket) => Future.successful(bucket)
    }

  def setBucketNotifications(notifications: List[Notification]): Future[Done] = {
    def createNotification(notification: Notification) =
      Future.successful(notificationCollection.createNotification(notification))

    Future.sequence(notifications.map(createNotification)).map(_ => Done)
  }

  def getBucketNotifications(bucketName: String): Future[List[Notification]] =
    Future.successful(notificationCollection.findNotifications(bucketName))

  def getAllObjects(objectId: UUID): Future[List[ObjectMeta]] =
    Future.successful(objectCollection.findAll(objectId))

  def createObject(bucket: Bucket,
                   key: String,
                   putObjectResult: PutObjectResult,
                   versionIndex: Int = 0): Future[CreateResponse] =
    Future.successful(objectCollection.createObject(bucket, key, putObjectResult, versionIndex))

  def close(): Unit = db.close()

}

object NitriteDatabase {
  def apply(dbSettings: DBSettings)(implicit system: ActorSystem[Nothing],
                                    dateTimeProvider: DateTimeProvider): NitriteDatabase =
    new NitriteDatabase(dbSettings)
}
