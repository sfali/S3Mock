package com.loyalty.testing.s3.repositories

import java.nio.file.Path

import akka.Done
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.collections.{BucketCollection, NotificationCollection, ObjectCollection}
import com.loyalty.testing.s3.response._
import com.loyalty.testing.s3.streams.FileStream
import org.dizitart.no2.Nitrite

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class NitriteRepository(dbSettings: DBSettings,
                        root: Path,
                        fileStream: FileStream,
                        log: LoggingAdapter)
                       (implicit mat: Materializer)
  extends Repository {

  import mat.executionContext
  import request._

  val workDir: Path = root.toAbsolutePath
  val dataDir: Path = workDir + "data"
  val uploadsDir: Path = workDir + "uploads"

  private val db: Nitrite = {
    val _db = Nitrite
      .builder()
      .compressed()
      .filePath(dbSettings.filePath)

    dbSettings.userName match {
      case Some(userName) => _db.openOrCreate(userName, dbSettings.password.get)
      case None => _db.openOrCreate()
    }
  }

  private[repositories] val bucketCollection = BucketCollection(db, dataDir)
  private[repositories] val notificationCollection = NotificationCollection(db)
  private[repositories] val objectCollection = ObjectCollection(db)

  override def createBucketWithVersioning(bucketName: String,
                                          bucketConfiguration: CreateBucketConfiguration,
                                          maybeBucketVersioning: Option[BucketVersioning]): Future[BucketResponse] =
    Try(bucketCollection.createBucket(bucketName, bucketConfiguration.locationConstraint,
      maybeBucketVersioning.map(_.toString))) match {
      case Failure(ex) => Future.failed(ex)
      case Success(response) => Future.successful(BucketResponse(bucketName, response.region,
        response.version))
    }

  override def createBucket(bucketName: String,
                            bucketConfiguration: CreateBucketConfiguration): Future[BucketResponse] =
    createBucketWithVersioning(bucketName, bucketConfiguration, None)

  override def setBucketVersioning(bucketName: String,
                                   contentSource: Source[ByteString, _]): Future[BucketResponse] =
    toVersionConfiguration(contentSource)
      .flatMap(versioningConfiguration => setBucketVersioning(bucketName, versioningConfiguration))

  override def setBucketVersioning(bucketName: String,
                                   versioningConfiguration: VersioningConfiguration): Future[BucketResponse] =
    Try(bucketCollection.setBucketVersioning(bucketName,
      versioningConfiguration.bucketVersioning.toString)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(response) => Future.successful(BucketResponse(bucketName, response.region,
        response.version))
    }

  override def setBucketNotification(bucketName: String,
                                     contentSource: Source[ByteString, _]): Future[Done] =
    toBucketNotification(bucketName, contentSource)
      .flatMap {
        notifications => setBucketNotification(bucketName, notifications)
      }

  override def setBucketNotification(bucketName: String,
                                     notifications: List[Notification]): Future[Done] = {
    def createNotification(notification: Notification) =
      Future.successful(notificationCollection.createNotification(notification))

    Try(bucketCollection.findBucket(bucketName)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.sequence(notifications.map(createNotification)).map(_ => Done)
    }
  }

  override def listBucket(bucketName: String,
                          params: ListBucketParams): Future[ListBucketResult] = ???

  override def putObject(bucketName: String,
                         key: String,
                         contentSource: Source[ByteString, _]): Future[ObjectMeta] = {
    val bucket = bucketCollection.findBucket(bucketName)
    saveObject(fileStream, key, bucket.bucketPath, bucket.version, contentSource)
      .map(response => objectCollection.createObject(bucket, response))
  }

  override def getObject(bucketName: String,
                         key: String,
                         maybeVersionId: Option[String],
                         maybeRange: Option[ByteRange]): Future[GetObjectResponse] = {
    val bucket = bucketCollection.findBucket(bucketName)
    val objectMeta = objectCollection.findObject(bucketName, key, maybeVersionId)
    getObjectPath(bucketName, key, bucket.bucketPath, objectMeta.path, maybeVersionId) match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) =>
        val sourceTuple = fileStream.downloadFile(objectMeta.path, maybeRange = maybeRange)
        Future.successful(GetObjectResponse(bucketName, key, objectMeta.result.etag, objectMeta.result.contentMd5,
          sourceTuple._1.capacity, sourceTuple._2, objectMeta.result.maybeVersionId))
    }
  }

  override def deleteObject(bucketName: String,
                            key: String,
                            maybeVersionId: Option[String]): Future[DeleteObjectResponse.type] = ???

  override def initiateMultipartUpload(bucketName: String,
                                       key: String): Future[InitiateMultipartUploadResult] = ???

  override def uploadMultipart(bucketName: String,
                               key: String,
                               partNumber: Int,
                               uploadId: String,
                               contentSource: Source[ByteString, _]): Future[ObjectMeta] = ???

  override def copyObject(bucketName: String,
                          key: String,
                          sourceBucketName: String,
                          sourceKey: String,
                          maybeSourceVersionId: Option[String]): Future[(ObjectMeta, CopyObjectResult)] = ???

  override def copyMultipart(bucketName: String,
                             key: String,
                             partNumber: Int,
                             uploadId: String,
                             sourceBucketName: String,
                             sourceKey: String,
                             maybeSourceVersionId: Option[String],
                             maybeSourceRange: Option[ByteRange]): Future[CopyPartResult] = ???

  override def completeMultipart(bucketName: String,
                                 key: String,
                                 uploadId: String,
                                 completeMultipartUpload: CompleteMultipartUpload): Future[CompleteMultipartUploadResult] = ???

  override def clean(): Unit = db.close()
}

object NitriteRepository {
  def apply(dbSettings: DBSettings,
            root: Path,
            log: LoggingAdapter)
           (implicit mat: Materializer): NitriteRepository =
    new NitriteRepository(dbSettings, root, FileStream(), log)
}