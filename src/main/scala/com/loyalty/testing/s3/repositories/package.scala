package com.loyalty.testing.s3

import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.UUID
import java.{lang, util}

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.request.{BucketVersioning, VersioningConfiguration}
import com.loyalty.testing.s3.response.{NoSuchKeyException, ObjectMeta}
import com.loyalty.testing.s3.streams.FileStream
import org.dizitart.no2.{Cursor, Document}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

package object repositories {

  val BucketNameField = "bucket-name"
  val IdField = "id"
  val RegionField = "region"
  val VersionField = "version"
  val NotificationNameField = "notification-name"
  val NotificationTypeField = "notification-type"
  val OperationTypeField = "operation-type"
  val DestinationTypeField = "destination-type"
  val DestinationNameField = "destination-name"
  val PrefixField = "prefix"
  val SuffixField = "suffix"
  val KeyField = "key"
  val ETagField = "etag"
  val ContentMd5Field = "content-md5"
  val ContentLengthField = "content-length"
  val VersionIndexField = "version-index"
  val VersionIdField = "version-id"
  val DeleteMarkerField = "delete-marker"
  val UploadIdField = "upload-id"
  val PartNumberField = "part-number"
  val NonVersionId: String = 0.toVersionId
  val ContentFileName: String = "content"

  implicit class LongOps(src: Long) {
    def toOffsetDateTime: OffsetDateTime = Instant.ofEpochMilli(src).atZone(ZoneId.systemDefault()).toOffsetDateTime
  }

  implicit class CursorOps(src: Cursor) {
    def toScalaList: List[Document] = src.asScala.toList
  }

  implicit class DocumentOps(src: Document) {
    def getString(key: String): String = src.get(key, classOf[String])

    def getOptionalString(key: String): Option[String] = Option(getString(key))

    def getUUID(key: String): UUID = UUID.fromString(getString(key))

    def getLong(key: String): Long = src.get(key, classOf[lang.Long]).toLong

    def getInt(key: String): Int = src.get(key, classOf[lang.Integer]).toInt

    def getBoolean(key: String): Boolean = src.get(key, classOf[Boolean])

    def getOptionalBoolean(key: String): Option[Boolean] =
      getOptionalString(key) match {
        case Some(value) => Some(value.toBoolean)
        case None => None
      }

    def getPath(key: String): Path = Paths.get(getString(key))

    def getOptionalForeignField(key: String): Option[List[Document]] =
      Option(src.get(key, classOf[util.HashSet[Document]])).map(_.asScala.toList)

    def toNotification: Notification =
      Notification(
        name = src.getString(NotificationNameField),
        notificationType = NotificationType.withName(src.getString(NotificationTypeField)),
        operationType = OperationType.withName(src.getString(OperationTypeField)),
        destinationType = DestinationType.withName(src.getString(DestinationTypeField)),
        destinationName = src.getString(DestinationNameField),
        bucketName = src.getString(BucketNameField),
        suffix = src.getOptionalString(SuffixField)
      )
  }

  def toVersionConfiguration(contentSource: Source[ByteString, _])
                            (implicit mat: Materializer): Future[VersioningConfiguration] =
    contentSource
      .map(_.utf8String)
      .map(s => if (s.isEmpty) None else Some(s))
      .map(VersioningConfiguration(_))
      .map {
        case Some(versioningConfiguration) => versioningConfiguration
        case None => VersioningConfiguration(BucketVersioning.Suspended)
      }
      .runWith(Sink.head)

  def toBucketNotification(bucketName: String, contentSource: Source[ByteString, _])
                          (implicit mat: Materializer): Future[List[Notification]] =
    contentSource
      .map(_.utf8String)
      .map(s => parseNotificationConfiguration(bucketName, s))
      .runWith(Sink.head)

  def getDestinationPathWithVersionId(key: String,
                                      bucketPath: Path,
                                      maybeBucketVersioning: Option[BucketVersioning]): (Option[String], Path) = {
    val parentPath = bucketPath -> key

    val maybeVersioningConfiguration = maybeBucketVersioning.filter(_ == BucketVersioning.Enabled)
    val (maybeVersionId, filePath) =
      maybeVersioningConfiguration match {
        case Some(_) =>
          val versionId = toBase16FromRandomUUID
          (Some(versionId), parentPath -> (versionId, ContentFileName))
        case None =>
          (None, parentPath -> (NonVersionId, ContentFileName))
      }
    Files.createDirectories(filePath.getParent)
    (maybeVersionId, filePath)
  }

  def saveObject(fileStream: FileStream,
                 key: String,
                 bucketPath: Path,
                 maybeBucketVersioning: Option[BucketVersioning],
                 contentSource: Source[ByteString, _])
                (implicit ec: ExecutionContext): Future[ObjectMeta] = {
    val (maybeVersionId, filePath) = getDestinationPathWithVersionId(key, bucketPath, maybeBucketVersioning)
    fileStream.saveContent(contentSource, filePath)
      .flatMap {
        case (etag, contentMD5, length) =>
          if (Files.notExists(filePath)) Future.failed(new RuntimeException("unable to save file"))
          else Future.successful(ObjectMeta(filePath,
            createPutObjectResult(key, etag, contentMD5, length, maybeVersionId)))
      }
  }

  def getObjectPath(bucketName: String,
                    key: String,
                    bucketPath: Path,
                    objectPath: Path,
                    maybeVersionId: Option[String] = None): Try[Path] = {
    val _objectPath = maybeVersionId.map(versionId => (bucketPath -> key) -> (versionId, ContentFileName))
      .getOrElse(objectPath)
    if (Files.notExists(_objectPath)) Failure(NoSuchKeyException(bucketName, key))
    else Success(_objectPath)
  }


}
