package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.{ListBucketResultContents, S3Headers}
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.loyalty.testing.s3.it._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.response.{CopyObjectResult, CopyPartResult}
import com.loyalty.testing.s3.{data, _}
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{BucketVersioningStatus, NoSuchKeyException}

import scala.concurrent.Future

class AlpakkaClient(override protected val awsSettings: AwsSettings)
                   (implicit system: ActorSystem[Nothing]) extends S3Client {

  import Framing.delimiter
  import system.executionContext

  private val windowsSplitter = delimiter(ByteString("\r\n"), maximumFrameLength = 1024, allowTruncation = true)

  private val awsClient = AwsClient(awsSettings)

  override def createBucket(bucketName: String, region: Option[Region]): Future[Bucket] =
    awsClient.createBucket(bucketName, region)

  override def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done] =
    awsClient.setBucketVersioning(bucketName, status)

  def listObjects(bucketName: String,
                  delimiter: Option[String],
                  prefix: Option[String],
                  maxKeys: Int = 1000): Source[ListBucketResultContents, NotUsed] = {
    S3.listBucket(bucketName, prefix)
  }

  override def putObject(bucketName: String, key: String, filePath: Path): Future[data.ObjectInfo] = {
    val contentLength = Files.size(filePath)
    S3.putObject(bucketName, key, FileIO.fromPath(filePath), contentLength, s3Headers = S3Headers())
      .map {
        objectMetadata =>
          data.ObjectInfo(
            bucketName = bucketName,
            key = key,
            eTag = objectMetadata.eTag,
            contentLength = contentLength,
            versionId = objectMetadata.versionId,
          )
      }
      .runWith(Sink.head)
  }

  override def getObject(bucketName: String,
                         key: String,
                         maybeVersionId: Option[String],
                         maybeRange: Option[ByteRange]): Future[(String, data.ObjectInfo)] = {
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
          val objectKey = data.ObjectInfo(
            bucketName = bucketName,
            key = key,
            eTag = objectMetadata.eTag,
            contentLength = objectMetadata.getContentLength,
            versionId = objectMetadata.versionId
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

  override def deleteObject(bucketName: String,
                            key: String,
                            maybeVersionId: Option[String]): Future[(Option[Boolean], Option[String])] =
    awsClient.deleteObject(bucketName, key, maybeVersionId)

  override def multiPartUpload(bucketName: String,
                               key: String,
                               totalSize: Int): Future[data.ObjectInfo] =
    createContentSource(1, totalSize)
      .runWith(S3.multipartUpload(bucketName, key))
      .map(result => data.ObjectInfo(
        bucketName = result.bucket,
        key = result.key,
        eTag = Some(result.etag),
        contentLength = 0,
        versionId = result.versionId
      ))

  override def copyObject(sourceBucketName: String,
                          sourceKey: String,
                          targetBucketName: String,
                          targetKey: String,
                          maybeSourceVersionId: Option[String]): Future[CopyObjectResult] =
    awsClient.copyObject(sourceBucketName, sourceKey, targetBucketName, targetKey, maybeSourceVersionId)

  override def multiPartCopy(sourceBucketName: String,
                             sourceKey: String,
                             targetBucketName: String,
                             targetKey: String,
                             maybeSourceVersionId: Option[String]): Future[CopyPartResult] =
    S3
      .multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, maybeSourceVersionId)
      .mapMaterializedValue(_.map(result => CopyPartResult(result.etag, result.versionId)))
      .run()

  /*private def getHeader(headers: Seq[HttpHeader], headerName: String): Option[HttpHeader] =
    headers.find(_.lowercaseName() == headerName.toLowerCase)*/
}

object AlpakkaClient {
  def apply()(implicit system: ActorSystem[Nothing],
              settings: ITSettings): AlpakkaClient =
    new AlpakkaClient(settings.awsSettings)
}
