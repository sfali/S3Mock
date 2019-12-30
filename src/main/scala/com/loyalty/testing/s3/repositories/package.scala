package com.loyalty.testing.s3

import java.nio.file.Path
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.UUID
import java.{lang, util}

import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import org.dizitart.no2.{Cursor, Document}

import scala.jdk.CollectionConverters._

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
  val PathField = "path"
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
  val NonVersionId: UUID => String = objectId => createVersionId(objectId, 0)
  val ContentFileName = "content"

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

    def getPath(key: String): Path = getString(key).toPath

    def getOptionalPath(key: String): Option[Path] = getOptionalString(key).map(_.toPath)

    def getOptionalForeignField(key: String): Option[List[Document]] =
      Option(src.get(key, classOf[util.HashSet[Document]])).map(_.asScala.toList)

    def getListField(key: String): List[String] = src.get(key, classOf[util.ArrayList[String]]).asScala.toList

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

}
