package com.loyalty.testing.s3.repositories

import java.nio.file.Path
import java.util.UUID

import akka.Done
import akka.actor.typed.ActorSystem
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.collections.{BucketCollection, NotificationCollection, ObjectCollection, UploadStagingCollection}
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, UploadInfo}
import com.loyalty.testing.s3.request.VersioningConfiguration
import com.loyalty.testing.s3.settings.Settings
import com.loyalty.testing.s3.utils.DateTimeProvider
import com.loyalty.testing.s3.{DBSettings, _}
import org.dizitart.no2.Nitrite
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class NitriteDatabase(rootPath: Path,
                      dbSettings: DBSettings)(implicit system: ActorSystem[Nothing],
                                              dateTimeProvider: DateTimeProvider) {

  import system.executionContext

  private val log = LoggerFactory.getLogger(classOf[NitriteDatabase])

  private val db: Nitrite = {
    val _db = Nitrite
      .builder()
      .compressed()
      .filePath((rootPath -> dbSettings.fileName).toString)

    dbSettings.userName match {
      case Some(userName) => _db.openOrCreate(userName, dbSettings.password.getOrElse(userName))
      case None => _db.openOrCreate()
    }
  }

  private[repositories] val bucketCollection = BucketCollection(db)
  private[repositories] val notificationCollection = NotificationCollection(db)
  private[repositories] val objectCollection = ObjectCollection(db)
  private[repositories] val uploadCollection = UploadStagingCollection(db)

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

  def setBucketVersioning(bucketId: UUID, versioningConfiguration: VersioningConfiguration): Future[Bucket] = {
    log.info("Setting bucket versioning on bucket_id={}, config={}", bucketId, versioningConfiguration)
    Try(bucketCollection.setBucketVersioning(bucketId, versioningConfiguration.bucketVersioning)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(bucket) => Future.successful(bucket)
    }
  }

  def setBucketNotifications(notifications: List[Notification]): Future[Done] = {
    def createNotification(notification: Notification) =
      Future.successful(notificationCollection.createNotification(notification))

    Future.sequence(notifications.map(createNotification)).map(_ => Done)
  }

  def getBucketNotifications(bucketName: String): Future[List[Notification]] =
    Future.successful(notificationCollection.findNotifications(bucketName))

  def getAllObjects(objectId: UUID): Future[List[ObjectKey]] =
    Future.successful(objectCollection.findAll(objectId))

  def createObject(objectKey: ObjectKey): Future[ObjectKey] = Future.successful(objectCollection.createObject(objectKey))

  def deleteObject(objectId: UUID, maybeVersionId: Option[String], permanentDelete: Boolean): Future[Done] =
    Try(objectCollection.deleteObject(objectId, maybeVersionId, permanentDelete)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }

  def createUpload(uploadInfo: UploadInfo): Future[Done] =
    Try(uploadCollection.createUpload(uploadInfo)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }

  def deleteUpload(uploadId: String, partNumber: Int): Future[Done] =
    Try(uploadCollection.deleteUpload(uploadId, partNumber)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }

  def close(): Unit = db.close()

}

object NitriteDatabase {
  def apply(rootPath: Path, dbSettings: DBSettings)
           (implicit system: ActorSystem[Nothing],
            dateTimeProvider: DateTimeProvider): NitriteDatabase = new NitriteDatabase(rootPath, dbSettings)

  def apply(rootPath: Path)
           (implicit system: ActorSystem[Nothing],
            dateTimeProvider: DateTimeProvider,
            settings: Settings): NitriteDatabase = NitriteDatabase(rootPath, settings.dbSettings)
}
