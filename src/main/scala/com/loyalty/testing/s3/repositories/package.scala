package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}
import java.util

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.request.{BucketVersioning, VersioningConfiguration}
import org.dizitart.no2.{Cursor, Document}

import scala.collection.JavaConverters._
import scala.concurrent.Future

package object repositories {

  val BucketNameField = "bucket-name"
  val RegionField = "region"
  val BucketPathField = "bucket-path"
  val VersionField = "version"
  val NotificationNameField = "notification-name"
  val NotificationTypeField = "notification-type"
  val OperationTypeField = "operation-type"
  val DestinationTypeField = "destination-type"
  val DestinationNameField = "destination-name"
  val PrefixField = "prefix"
  val SuffixField = "suffix"

  implicit class CursorOps(src: Cursor) {
    def toScalaList: List[Document] = src.asScala.toList
  }

  implicit class DocumentOps(src: Document) {
    def getString(key: String): String = src.get(key, classOf[String])

    def getBoolean(key: String): Boolean = src.get(key, classOf[Boolean])

    def getOptionalString(key: String): Option[String] = Option(getString(key))

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
        prefix = src.getOptionalString(PrefixField),
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

}
