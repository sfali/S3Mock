package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.Done
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.it._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.{BucketAlreadyExistsException, NoSuchBucketException}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{BucketAlreadyExistsException => AwsBucketAlreadyExistsException, NoSuchBucketException => AwsNoSuchBucketException, _}
import software.amazon.awssdk.services.s3.{S3Configuration, S3Client => AwsS3Client}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AwsClient(override protected val awsSettings: AwsSettings) extends S3Client {

  private val s3Client = AwsS3Client
    .builder()
    .region(awsSettings.region)
    .credentialsProvider(awsSettings.credentialsProvider)
    .endpointOverride(awsSettings.s3EndPoint.get)
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .build()

  override def createBucket(bucketName: String, region: Option[Region]): Future[Bucket] = {
    val requestBuilder = CreateBucketRequest.builder().bucket(bucketName)
    val request =
      region match {
        case Some(value) =>
          val c = CreateBucketConfiguration.builder().locationConstraint(value.id()).build()
          requestBuilder.createBucketConfiguration(c).build()
        case None => requestBuilder.build()
      }
    Try(s3Client.createBucket(request)) match {
      case Failure(_: AwsBucketAlreadyExistsException) => Future.failed(BucketAlreadyExistsException(bucketName))
      case Failure(ex) => Future.failed(ex)
      case Success(resp) =>
        val location = resp.location().drop(1)
        Future.successful(Bucket(location, region.map(_.id()).getOrElse(defaultRegion),
          BucketVersioning.NotExists))
    }
  }

  override def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done] = {
    val vc = VersioningConfiguration.builder().status(status).build()
    val request = PutBucketVersioningRequest.builder().bucket(bucketName).versioningConfiguration(vc).build()
    Try(s3Client.putBucketVersioning(request)) match {
      case Failure(_: AwsNoSuchBucketException) => Future.failed(NoSuchBucketException(bucketName))
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }
  }

  override def putObject(bucketName: String,
                         key: String,
                         contentMd5: String,
                         filePath: Path): Future[ObjectKey] = {
    val contentLength = Files.size(filePath)
    val request = PutObjectRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .contentLength(contentLength)
      .contentMD5(contentMd5)
      .build()
    Try(s3Client.putObject(request, filePath)) match {
      case Failure(_: AwsNoSuchBucketException) => Future.failed(NoSuchBucketException(bucketName))
      case Failure(ex) => Future.failed(ex)
      case Success(response) =>
        val objectKey = ObjectKey(
          id = bucketName.toUUID,
          bucketName = bucketName,
          key = key,
          index = 0,
          version = BucketVersioning.NotExists,
          versionId = response.versionId(),
          eTag = response.eTag(),
          contentMd5 = contentMd5,
          contentLength = contentLength,
          lastModifiedTime = dateTimeProvider.currentOffsetDateTime
        )
        Future.successful(objectKey)
    }
  }
}

object AwsClient {
  def apply(awsSettings: AwsSettings): AwsClient = new AwsClient(awsSettings)

  def apply()(implicit settings: ITSettings): AwsClient = AwsClient(settings.awsSettings)
}
