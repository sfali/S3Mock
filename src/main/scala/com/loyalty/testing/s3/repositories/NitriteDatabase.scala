package com.loyalty.testing.s3.repositories

import java.nio.file.Path
import java.util.UUID

import akka.Done
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.collections._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, UploadInfo}
import com.loyalty.testing.s3.request.{BucketVersioning, VersioningConfiguration}
import com.loyalty.testing.s3.settings.Settings
import com.loyalty.testing.s3.utils.DateTimeProvider
import com.loyalty.testing.s3.{DBSettings, _}
import org.dizitart.no2.Nitrite
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class NitriteDatabase(rootPath: Path,
                      dbSettings: DBSettings)(implicit dateTimeProvider: DateTimeProvider) {

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
  private[repositories] val uploadStagingCollection = UploadCollection(db, staging = true)
  private[repositories] val uploadCollection = UploadCollection(db, staging = false)

  def findBucket(id: UUID): Option[Bucket] = bucketCollection.findBucket(id)

  def getAllObjects(bucketName: String): List[ObjectKey] = objectCollection.findAll(bucketName)

  def createBucket(bucket: Bucket): Bucket = bucketCollection.createBucket(bucket)

  def deleteBucket(bucketName: String): Unit = {
    if (objectCollection.hasObjects(bucketName)) throw BucketNotEmptyException(bucketName)
    else {
      objectCollection.deleteAll(bucketName)
      bucketCollection.deleteBucket(bucketName)
      ()
    }
  }

  def setBucketVersioning(bucketId: UUID,
                          versioningConfiguration: VersioningConfiguration): Option[Bucket] = {
    log.info("Setting bucket versioning on bucket_id={}, config={}", bucketId, versioningConfiguration)
    bucketCollection.setBucketVersioning(bucketId, versioningConfiguration.bucketVersioning)
  }

  def setBucketNotifications(notifications: List[Notification]): Int =
    notificationCollection.createNotifications(notifications)

  def getBucketNotifications(bucketName: String): List[Notification] =
    notificationCollection.findNotifications(bucketName)

  def getAllObjects(objectId: UUID): Future[List[ObjectKey]] =
    Future.successful(objectCollection.findAll(objectId))

  def createOrUpdateObject(objectKey: ObjectKey): Future[ObjectKey] = Future.successful(objectCollection.createOrUpdateObject(objectKey))

  def findUploads: Future[List[UploadInfo]] = Future.successful(uploadStagingCollection.findAll)

  def createUpload(uploadInfo: UploadInfo): Future[Done] =
    Try(uploadStagingCollection.createUpload(uploadInfo)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }

  def moveParts(objectKey: ObjectKey): Future[Done] = {
    val uploadId = objectKey.uploadId.get
    Try {
      if (BucketVersioning.Enabled != objectKey.version) uploadCollection.deleteAll(uploadId)
      val allUploads = uploadStagingCollection.findAll(uploadId).tail
      uploadCollection.insert(allUploads: _*)
      uploadStagingCollection.deleteAll(uploadId)
    } match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }
  }

  def deleteUpload(uploadId: String, partNumber: Int): Future[Done] =
    Try(uploadStagingCollection.deleteUpload(uploadId, partNumber)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }

  def deleteUpload(maybeUploadId: Option[String]): Future[Int] =
    maybeUploadId match {
      case Some(uploadId) =>
        Try(uploadCollection.deleteAll(uploadId)) match {
          case Failure(ex) =>
            log.error(s"Unable to delete upload: $uploadId", ex)
            Future.successful(0)
          case Success(value) => Future.successful(value)
        }
      case None => Future.successful(0)
    }

  def findNotifications(bucketName: String): List[Notification] = notificationCollection.findNotifications(bucketName)

  def deleteNotifications(bucketName: String): Int = notificationCollection.deleteNotifications(bucketName)

  def close(): Unit = db.close()

}

object NitriteDatabase {
  def apply(rootPath: Path, dbSettings: DBSettings)
           (implicit dateTimeProvider: DateTimeProvider): NitriteDatabase = new NitriteDatabase(rootPath, dbSettings)

  def apply()(implicit dateTimeProvider: DateTimeProvider,
              settings: Settings): NitriteDatabase = NitriteDatabase(settings.dataDirectory, settings.dbSettings)
}
