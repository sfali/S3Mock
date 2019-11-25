package com.loyalty.testing

import java.net.{URI, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.CompletableFuture

import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import akka.http.scaladsl.model.headers.{ByteRange, RawHeader}
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.request.UploadPart
import com.loyalty.testing.s3.response.{CompleteMultipartUploadResult, InvalidNotificationConfigurationException, PutObjectResult}
import com.typesafe.config.Config
import javax.xml.bind.DatatypeConverter
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.xml.{Node, NodeSeq}

package object s3 {

  import scala.compat.java8.FutureConverters._

  type JavaFuture[V] = java.util.concurrent.Future[V]

  val defaultRegion: String = "us-east-1"

  val ETAG = "ETag"
  val CONTENT_MD5 = "Content-MD5"
  val Content_Length = "Content-Length"

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
    def decode: String = URLDecoder.decode(s, UTF_8.toString)

    def encode: String = URLEncoder.encode(s, UTF_8.toString)

    def toDestinationName: String = {
      val lastColon = s.lastIndexOf(':')
      if (lastColon > -1) s.substring(lastColon + 1) else s
    }

    def toPath: Path = Paths.get(s)

    def toUUID: UUID = UUID.nameUUIDFromBytes(s.getBytes)
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
                                          parts: List[UploadPart],
                                          maybeVersionId: Option[String] = None): CompleteMultipartUploadResult = {
    val hex = toBase16(parts.map(_.eTag).mkString)
    val eTag = s"$hex-${parts.length}"

    CompleteMultipartUploadResult(bucketName, key, eTag, 0L, maybeVersionId)
  }

  def createPutObjectResult(filePath: String,
                            eTag: String,
                            contentMd5: String,
                            contentLength: Long,
                            maybeVersionId: Option[String] = None): PutObjectResult = {
    PutObjectResult(filePath, eTag, contentMd5, contentLength, maybeVersionId)
  }

  implicit class JavaFutureOps[T](future: JavaFuture[T]) {
    def toScalaFuture: Future[T] = CompletableFuture.supplyAsync(() => future.get()).toScala
  }

  trait SqsSettings {
    val sqsClient: AmazonSQSAsync
  }

  trait SnsSettings {
    val snsClient: AmazonSNSAsync
  }

  case class DownloadRange(startPosition: Long, endPosition: Long, capacity: Long)

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

  def createObjectId(bucketName: String, key: String): UUID = s"$bucketName-$key".toUUID

  implicit class IntOps(src: Int) {
    def toVersionId: String = toBase16(src.toString.toUUID.toString)
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

}
