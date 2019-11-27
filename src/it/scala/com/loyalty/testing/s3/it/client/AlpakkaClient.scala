package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.Done
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{FileIO, Framing, Sink}
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.it._
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import com.loyalty.testing.s3.request.BucketVersioning
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{BucketVersioningStatus, NoSuchKeyException}

import scala.concurrent.Future

class AlpakkaClient(override protected val awsSettings: AwsSettings)
                   (implicit system: ActorSystem[Nothing]) extends S3Client {

  import Framing.delimiter

  private val windowsSplitter = delimiter(ByteString("\r\n"), maximumFrameLength = 1024, allowTruncation = true)

  private val awsClient = AwsClient(awsSettings)

  override def createBucket(bucketName: String, region: Option[Region]): Future[Bucket] =
    awsClient.createBucket(bucketName, region)

  override def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done] =
    awsClient.setBucketVersioning(bucketName, status)

  override def putObject(bucketName: String, key: String, filePath: Path): Future[ObjectKey] = {
    val contentLength = Files.size(filePath)
    S3.putObject(bucketName, key, FileIO.fromPath(filePath), contentLength, s3Headers = S3Headers())
      .map {
        objectMetadata =>
          ObjectKey(
            id = bucketName.toUUID,
            bucketName = bucketName,
            key = key,
            index = 0,
            version = BucketVersioning.NotExists,
            versionId = objectMetadata.versionId.getOrElse(NonVersionId),
            eTag = objectMetadata.eTag.getOrElse(""),
            contentMd5 = getHeader(objectMetadata.metadata, CONTENT_MD5).map(_.value()).getOrElse(""),
            contentLength = contentLength,
            lastModifiedTime = dateTimeProvider.currentOffsetDateTime
          )
      }.runWith(Sink.head)
  }

  def getObject(bucketName: String,
                key: String,
                maybeVersionId: Option[String],
                maybeRange: Option[ByteRange]): Future[(String, ObjectKey)] = {
    import system.executionContext
    S3.download(bucketName, key, maybeRange, maybeVersionId)
      .map {
        case None =>
          val errorDetails =
            AwsErrorDetails
              .builder()
              .errorCode("NoSuchKey")
              .errorMessage("The resource you requested does not exist")
              .build()
          throw NoSuchKeyException.builder()
            .statusCode(404)
            .message("The resource you requested does not exist")
            .awsErrorDetails(errorDetails)
            .build()
        case Some((source, objectMetadata)) =>
          val objectKey = ObjectKey(
            id = bucketName.toUUID,
            bucketName = bucketName,
            key = key,
            index = 0,
            version = BucketVersioning.NotExists,
            versionId = objectMetadata.versionId.getOrElse(NonVersionId),
            eTag = objectMetadata.eTag.getOrElse(""),
            contentMd5 = getHeader(objectMetadata.metadata, CONTENT_MD5).map(_.value()).getOrElse(""),
            contentLength = objectMetadata.getContentLength,
            lastModifiedTime = dateTimeProvider.currentOffsetDateTime
          )
          (source, objectKey)
      }.runWith(Sink.head)
      .flatMap {
        case (source, objectKey) =>
          source
            .via(windowsSplitter)
            .map(_.utf8String)
            .map(_ + "\r\n")
            .runWith(Sink.seq)
            .map(_.mkString(""))
            .map(s => (s, objectKey))
      }
  }

  private def getHeader(headers: Seq[HttpHeader], headerName: String): Option[HttpHeader] =
    headers.find(_.lowercaseName() == headerName.toLowerCase)
}

object AlpakkaClient {
  def apply()(implicit system: ActorSystem[Nothing],
              settings: ITSettings): AlpakkaClient =
    new AlpakkaClient(settings.awsSettings)
}
