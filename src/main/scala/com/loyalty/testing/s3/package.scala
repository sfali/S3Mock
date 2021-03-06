package com.loyalty.testing

import java.io.IOException
import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.UUID

import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import akka.http.scaladsl.model.headers.{ByteRange, RawHeader}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.data.{BootstrapConfiguration, InitialBucket}
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.{BucketVersioning, PartInfo}
import com.loyalty.testing.s3.response.{CompleteMultipartUploadResult, InvalidNotificationConfigurationException}
import com.loyalty.testing.s3.utils.StringUtils
import com.typesafe.config.Config
import io.circe.generic.auto._
import io.circe.parser._
import javax.xml.bind.DatatypeConverter
import org.slf4j.Logger
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.xml.{Node, NodeSeq}

package object s3 {

  val defaultRegion: String = "us-east-1"
  val UserDir: Path = System.getProperty("user.dir").toPath
  val ETAG = "ETag"
  val CONTENT_MD5 = "Content-MD5"
  val ContentRangeHeader = "Content-Range"
  val VersionIdHeader = "x-amz-version-id"
  val SourceVersionIdHeader = "x-amz-copy-source-version-id"
  val DeleteMarkerHeader = "x-amz-delete-marker"
  val PartsCountHeader = "x-amz-mp-parts-count"

  private val md = MessageDigest.getInstance("MD5")

  def toBase16(s: String): String = toBase16(s.getBytes)

  def toBase16(bytes: Array[Byte]): String = {
    md.reset()
    md.update(bytes)
    DatatypeConverter.printHexBinary(md.digest()).toLowerCase
  }

  def toBase64(s: String): String = toBase64(s.getBytes)

  def toBase64(bytes: Array[Byte]): String = {
    md.reset()
    md.update(bytes)
    DatatypeConverter.printBase64Binary(md.digest())
  }

  def md5Hex(path: Path): String = toBase16(Files.readAllBytes(path))

  def toBase16FromRandomUUID: String = toBase16(UUID.randomUUID().toString)

  implicit class StringOps(s: String) {
    def decode: String = StringUtils.decode(s, UTF_8.toString)

    def encode: String = URLEncoder.encode(s, UTF_8.toString)

    def toDestinationName: String = {
      val lastColon = s.lastIndexOf(':')
      if (lastColon > -1) s.substring(lastColon + 1) else s
    }

    def toPath: Path = Paths.get(s)

    def toUUID: UUID = UUID.nameUUIDFromBytes(s.getBytes)

    def replaceNewLine: String = s.replaceAll(System.lineSeparator(), "")

    def toOption: Option[String] = if (s.isEmpty) None else Some(s)

    def parseEtag: String = {
      val s1 = if (s.startsWith("\"")) s.drop(1) else s
      if (s1.endsWith("\"")) s1.dropRight(1) else s1
    }
  }

  implicit class PathOps(path: Path) {

    /**
      * Append the given `other` path and create directories if not exists.
      *
      * @param other other path to append
      * @return new path
      */
    def +(other: Path): Path = {
      val result = Paths.get(path.toString, other.toString)
      createDirectories(result)
      result
    }

    def +(other: String*): Path = {
      val result = Paths.get(path.toString, other: _*)
      createDirectories(result)
      result
    }

    def ->(other: String*): Path = Paths.get(path.toString, other: _*)

    def toUnixPath: String = path.toString.replaceAll("\\\\", "/")
  }

  def createCompleteMultipartUploadResult(bucketName: String,
                                          key: String,
                                          parts: List[PartInfo],
                                          maybeVersionId: Option[String] = None): CompleteMultipartUploadResult = {
    val hex = toBase16(parts.map(_.eTag).mkString)
    val eTag = s"$hex-${parts.length}"

    CompleteMultipartUploadResult(bucketName, key, eTag, 0L, maybeVersionId)
  }

  case class DownloadRange(startPosition: Long, endPosition: Long, capacity: Long) {
    def toByteRange: Slice = ByteRange(startPosition, endPosition - 1)
  }

  object DownloadRange {
    def apply(path: Path, maybeRange: Option[ByteRange] = None): DownloadRange = {
      val contentLength: Long = Files.size(path)
      maybeRange.map {
        case Slice(first, last) => DownloadRange(first, last, last - first)
        case FromOffset(offset) =>
          val first = offset
          val last = contentLength
          DownloadRange(first, last, last - first)
        case Suffix(length) =>
          val first = contentLength - length
          val last = contentLength
          DownloadRange(first, last, last - first)
      }.getOrElse(DownloadRange(0, contentLength, contentLength))
    }
  }

  private def createDirectories(path: Path): Unit = {
    if (Files.notExists(path)) Files.createDirectories(path)
  }

  implicit class HeaderOps(headers: List[RawHeader]) {
    def +(key: String, value: String): List[RawHeader] = headers :+ RawHeader(key, value)

    def +(key: String, maybeValue: Option[String]): List[RawHeader] =
      maybeValue.map(value => headers :+ RawHeader(key, value)).getOrElse(headers)
  }

  def toBucketNotification(bucketName: String, contentSource: Source[ByteString, _])
                          (implicit mat: Materializer): Future[List[Notification]] =
    contentSource
      .map(_.utf8String)
      .map(s => parseNotificationConfiguration(bucketName, s))
      .runWith(Sink.head)

  def parseNotificationConfiguration(bucketName: String, xml: String): List[Notification] = {
    val node = scala.xml.XML.loadString(xml)
    val configurations = node.child.filterNot(_.label == "#PCDATA")
    configurations.flatMap {
      node =>
        val idNode = node \ "Id"
        val id = if (idNode.isEmpty) UUID.randomUUID().toString else idNode.head.text

        val destinationType = toDestinationType(bucketName, node.label)
        val destinationName = toDestinationName(node, bucketName, destinationType)
        val (prefix, suffix) = parseFilter(node \ "Filter", bucketName)

        val notification =
          Notification(
            name = id,
            destinationType = destinationType,
            destinationName = destinationName,
            bucketName = bucketName,
            prefix = prefix,
            suffix = suffix
          )
        parseEvents(node \ "Event", notification)
    }
      .toList
  }

  private def toDestinationName(node: Node,
                                bucketName: String,
                                destinationType: DestinationType) = {
    def parseDestinationName(elementName: String) = {
      (node \ elementName).headOption match {
        case Some(value) => value.text.toDestinationName
        case None => throw InvalidNotificationConfigurationException(bucketName, s"Missing required parameter: $elementName")
      }
    }

    destinationType match {
      case DestinationType.Sqs => parseDestinationName("Queue")
      case DestinationType.Sns => parseDestinationName("Topic")
      case DestinationType.Cloud => parseDestinationName("CloudFunction")
    }
  }

  private def parseFilter(nodeSeq: NodeSeq, bucketName: String) = {
    if (nodeSeq.isEmpty) (None, None)
    else {
      val filterRuleNodeSeq = nodeSeq.head \ "S3Key" \ "FilterRule"
      if (filterRuleNodeSeq.isEmpty) (None, None)
      else {
        val parsedFilters = filterRuleNodeSeq.map(parseFilterRule(bucketName))
        val filterPrefix = parsedFilters.filter(_._1 == "prefix").map(_._2).toSet.toList
        if (filterPrefix.length > 1) {
          throw InvalidNotificationConfigurationException(bucketName, "Cannot specify more than one prefix rule in a filter")
        }
        val filterSuffix = parsedFilters.filter(_._1 == "suffix").map(_._2).toSet.toList
        if (filterSuffix.length > 1) {
          throw InvalidNotificationConfigurationException(bucketName, "Cannot specify more than one prefix rule in a filter")
        }
        (filterPrefix.headOption, filterSuffix.headOption)
      }
    }
  }

  private def parseFilterRule(bucketName: String)(node: Node) = {
    val nameNodeSeq = node \ "Name"
    if (nameNodeSeq.isEmpty) throw InvalidNotificationConfigurationException(bucketName, "Missing required parameter: Name")
    val valueNodeSeq = node \ "Value"
    if (nameNodeSeq.isEmpty) throw InvalidNotificationConfigurationException(bucketName, "Missing required parameter: Value")

    val name = nameNodeSeq.head.text
    val value = valueNodeSeq.head.text
    if (name != "prefix" && name != "suffix")
      throw InvalidNotificationConfigurationException(bucketName, "filter rule name must be either prefix or suffix")
    (name, value)
  }

  private def parseEvents(eventNodes: NodeSeq, notification: Notification) =
    if (eventNodes.isEmpty) throw InvalidNotificationConfigurationException(notification.bucketName, "Missing required parameter: Event")
    else {
      eventNodes.map {
        eventNode =>
          val (notificationType, operationType) = parseEvent(eventNode.text, notification.bucketName)
          notification.copy(notificationType = notificationType, operationType = operationType)
      }
    }

  private def parseEvent(event: String, bucketName: String) = {
    val values = event.split(":")
    if (values.isEmpty || values.length <= 2 || values.head != "s3")
      throw InvalidNotificationConfigurationException(bucketName, "The event is not supported for notifications")
    else {
      val notificationType = NotificationType.withNameOption(values(1))
      val operationType = OperationType.withNameOption(values.last)
      if (notificationType.isEmpty || operationType.isEmpty)
        throw InvalidNotificationConfigurationException(bucketName, "The event is not supported for notifications")
      else (notificationType.get, operationType.get)
    }
  }

  private def toDestinationType(bucketName: String, s: String): DestinationType =
    if (s == "QueueConfiguration") DestinationType.Sqs
    else if (s == "TopicConfiguration") DestinationType.Sns
    else if (s == "CloudFunctionConfiguration") DestinationType.Cloud
    else throw InvalidNotificationConfigurationException(bucketName,
      s""""$s", must be one of: TopicConfiguration, QueueConfiguration, CloudFunctionConfiguration""")


  trait HttpSettings {
    val host: String
    val port: Int
  }

  object HttpSettings {
    def apply(config: Config): HttpSettings = new HttpSettings {
      override val host: String = config.getString("app.http.host")
      override val port: Int = config.getInt("app.http.port")
    }
  }

  trait AwsSettings {
    val region: Region
    val credentialsProvider: AwsCredentialsProvider
    val sqsEndPoint: Option[URI]
    val s3EndPoint: Option[URI]
    val snsEndPoint: Option[URI]
  }

  trait DBSettings {
    val fileName: String
    val userName: Option[String] = None
    val password: Option[String] = None
  }

  object DBSettings {
    def apply(config: Config): DBSettings = new DBSettings {
      override val fileName: String = config.getString("app.db.file-name")
      override val userName: Option[String] = config.getOptionalString("app.db-user-name")
      override val password: Option[String] = config.getOptionalString("app.db-password")
    }
  }

  def createObjectId(bucketName: String, key: String): UUID = s"$bucketName-$key".toUUID

  def entityId(bucket: Bucket, key: String): String = createObjectId(bucket.bucketName, key).toString

  def createUploadId(bucketName: String,
                     key: String,
                     version: BucketVersioning,
                     versionIndex: Int): String =
    toBase16(s"upload-$bucketName-$key-${version.entryName}-$versionIndex".toUUID.toString)

  def createVersionId(objectId: UUID, versionIndex: Int): String =
    toBase16(s"${objectId.toString}-$versionIndex".toUUID.toString)

  def toObjectDir(bucketName: String,
                  key: String,
                  bucketVersioning: BucketVersioning,
                  versionId: String,
                  uploadId: Option[String] = None): String = {
    val value = s"$bucketName-$key-${bucketVersioning.entryName}-$versionId"
    toBase16(uploadId.map(s => s"$value-$s").getOrElse(value).toUUID.toString)
  }

  implicit class ConfigOps(src: Config) {
    def getOptionalString(keyPath: String): Option[String] = {
      val maybeValue =
        if (src.hasPath(keyPath)) Some(src.getString(keyPath))
        else None

      maybeValue match {
        case Some(value) => if (value.trim.nonEmpty) Some(value.trim) else None
        case None => None
      }
    }

    def getOptionalUri(path: String): Option[URI] = {
      if (src.hasPath(path)) {
        val endPoint = src.getString(path)
        if (endPoint.isEmpty) None else Some(new URI(endPoint))
      } else None
    }

    def getFiniteDuration(path: String): FiniteDuration = FiniteDuration(src.getDuration(path).toMillis, MILLISECONDS)
  }

  def clean(rootPath: Path): Path =
    Files.walkFileTree(rootPath, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })

  def checkPartOrders(parts: List[Int]): Boolean =
    if (parts.isEmpty || (parts.head != 1 || parts.last != parts.length)) false
    else isSortedInternal(parts)

  @scala.annotation.tailrec
  private def isSortedInternal(ls: List[Int]): Boolean =
    ls match {
      case Nil | _ :: Nil => true
      case item1 :: item2 :: xs => item1 + 1 == item2 && isSortedInternal(item2 :: xs)
    }

  private def readInitialData(initialDataPath: Path, log: Logger): Option[BootstrapConfiguration] = {
    if (Files.exists(initialDataPath)) {
      log.info("Initial data found @ {}", initialDataPath)
      val json = Files.readAllLines(initialDataPath).asScala.mkString(System.lineSeparator())
      decode[BootstrapConfiguration](json) match {
        case Left(error) =>
          log.warn(s"Unable to parse json: $json", error)
          None
        case Right(bootstrapConfiguration) => Some(bootstrapConfiguration)
      }
    } else {
      log.info("No initial data found @ {}", initialDataPath)
      None
    }
  }


  private def initializeBucket(log: Logger,
                               database: NitriteDatabase)
                              (initialBucket: InitialBucket): Unit =
    Try(database.createBucket(Bucket(initialBucket))) match {
      case Failure(ex) =>
        log.warn("Unable to initialize bucket {}, error_message={}", initialBucket.bucketName, ex.getMessage)
      case Success(_) => log.info("Bucket initialized {}", initialBucket.bucketName)
    }

  private def initializeNotification(log: Logger,
                                     database: NitriteDatabase)
                                    (bucketName: String, notifications: List[Notification]): Unit =
    Try(database.setBucketNotifications(notifications)) match {
      case Failure(ex) =>
        log.warn("Unable to create notifications for bucket: {}, error_message={}", bucketName, ex.getMessage)
      case Success(_) => log.info("Notifications created for bucket {}", bucketName)
    }

  def initializeInitialData(initialDataPath: Path, log: Logger, database: NitriteDatabase): Unit =
    readInitialData(initialDataPath, log) match {
      case Some(bootstrapConfiguration) =>
        val initialBuckets = bootstrapConfiguration.initialBuckets
        if (initialBuckets.nonEmpty) {
          initialBuckets.foreach(initializeBucket(log, database))
          val notifications = bootstrapConfiguration.notifications.groupBy(_.bucketName)
          notifications.foreach((initializeNotification(log, database) _).tupled)
        } else log.warn("Initial buckets are not provided, skipping initialization")
      case None =>
    }

  @scala.annotation.tailrec
  def isTypeOf(ex: Throwable, cause: Class[_]): Boolean = {
    if (ex == null) false
    else if (cause.isAssignableFrom(ex.getClass)) true
    else isTypeOf(ex.getCause, cause)
  }
}
