package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.Done
import akka.actor.typed.ActorSystem
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{FileIO, Sink}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.it._
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import com.loyalty.testing.s3.request.BucketVersioning
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus

import scala.concurrent.Future

class AlpakkaClient(override protected val awsSettings: AwsSettings)
                   (implicit system: ActorSystem[Nothing]) extends S3Client {

  private val awsClient = AwsClient(awsSettings)

  override def createBucket(bucketName: String, region: Option[Region]): Future[Bucket] =
    awsClient.createBucket(bucketName, region)

  override def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done] =
    awsClient.setBucketVersioning(bucketName, status)

  override def putObject(bucketName: String, key: String, contentMd5: String, filePath: Path): Future[ObjectKey] = {
    val contentLength = Files.size(filePath)
    S3.putObject(bucketName, key, FileIO.fromPath(filePath), contentLength, s3Headers = S3Headers())
      .map {
        objectMetaData =>
          ObjectKey(
            id = bucketName.toUUID,
            bucketName = bucketName,
            key = key,
            index = 0,
            version = BucketVersioning.NotExists,
            versionId = objectMetaData.versionId.getOrElse(NonVersionId),
            eTag = objectMetaData.eTag.getOrElse(""),
            contentMd5 = contentMd5,
            contentLength = contentLength,
            lastModifiedTime = dateTimeProvider.currentOffsetDateTime
          )
      }.runWith(Sink.head)
  }
}

object AlpakkaClient {
  def apply()(implicit system: ActorSystem[Nothing],
              settings: ITSettings): AlpakkaClient =
    new AlpakkaClient(settings.awsSettings)
}
