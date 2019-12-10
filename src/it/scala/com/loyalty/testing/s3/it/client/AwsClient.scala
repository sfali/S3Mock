package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.Done
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.it._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
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
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }
  }

  override def putObject(bucketName: String,
                         key: String,
                         filePath: Path): Future[ObjectInfo] = {
    val contentLength = Files.size(filePath)
    val request = PutObjectRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .contentLength(contentLength)
      .build()
    Try(s3Client.putObject(request, filePath)) match {
      case Failure(ex) => Future.failed(ex)
      case Success(response) =>
        val objectInfo = ObjectInfo(
          bucketName = bucketName,
          key = key,
          eTag = response.eTag(),
          contentMd5 = "",
          contentLength = contentLength,
          versionId = Option(response.versionId()),
        )
        Future.successful(objectInfo)
    }
  }

  override def getObject(bucketName: String,
                         key: String,
                         maybeVersionId: Option[String],
                         maybeRange: Option[ByteRange]): Future[(String, ObjectInfo)] = {
    val range =
      maybeRange match {
        case Some(range: Slice) => s"${range.first}-${range.last}"
        case Some(range: FromOffset) => s"${range.offset}-"
        case Some(range: Suffix) => s"-${range.length}"
        case None => null
      }
    val request = GetObjectRequest.builder().bucket(bucketName).key(key).range(range).versionId(maybeVersionId.orNull)
      .build()
    val bytesResponse = s3Client.getObjectAsBytes(request)
    val response = bytesResponse.response()
    val md5 = Option(response.metadata().get(CONTENT_MD5)).getOrElse("")
    val objectInfo = ObjectInfo(
      bucketName = bucketName,
      key = key,
      eTag = response.eTag(),
      contentMd5 = md5,
      contentLength = response.contentLength(),
      versionId = Option(response.versionId())
    )

    Future.successful((bytesResponse.asUtf8String(), objectInfo))
  }

  override def deleteObject(bucketName: String,
                            key: String,
                            maybeVersionId: Option[String]): Future[(Option[Boolean], Option[String])] = {
    val request = DeleteObjectRequest.builder().bucket(bucketName).key(key).versionId(maybeVersionId.orNull).build()
    val response = s3Client.deleteObject(request)
    Future.successful((Option(response.deleteMarker()).map(_.booleanValue()), Option(response.versionId())))
  }
}

object AwsClient {
  def apply(awsSettings: AwsSettings): AwsClient = new AwsClient(awsSettings)

  def apply()(implicit settings: ITSettings): AwsClient = AwsClient(settings.awsSettings)
}
