package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.Done
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.it._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{CompletedPart, _}
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}

import scala.concurrent.Future
import scala.jdk.FutureConverters._

class AwsClient(override protected val awsSettings: AwsSettings)
               (implicit system: ActorSystem[Nothing])
  extends S3Client {

  import system.executionContext

  private val s3Client = S3AsyncClient
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
    s3Client.createBucket(request)
      .asScala
      .map {
        resp =>
          val location = resp.location().drop(1)
          Bucket(location, region.map(_.id()).getOrElse(defaultRegion), BucketVersioning.NotExists)
      }
  }

  override def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done] = {
    val vc = VersioningConfiguration.builder().status(status).build()
    val request = PutBucketVersioningRequest.builder().bucket(bucketName).versioningConfiguration(vc).build()
    s3Client.putBucketVersioning(request).asScala.map(_ => Done)
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
    s3Client.putObject(request, filePath).asScala
      .map {
        response =>
          ObjectInfo(
            bucketName = bucketName,
            key = key,
            eTag = response.eTag().drop(1).dropRight(1),
            contentMd5 = "",
            contentLength = contentLength,
            versionId = Option(response.versionId()),
          )
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
    val request = GetObjectRequest.builder().bucket(bucketName).key(key).range(s"bytes=$range")
      .versionId(maybeVersionId.orNull).build()
    s3Client.getObject(request, new ByteArrayAsyncResponseTransformer[GetObjectResponse]()).asScala
      .map {
        bytesResponse =>
          val response = bytesResponse.response()
          val objectInfo = ObjectInfo(
            bucketName = bucketName,
            key = key,
            eTag = response.eTag().drop(1).dropRight(1),
            contentMd5 = "",
            contentLength = response.contentLength(),
            versionId = Option(response.versionId())
          )
          (bytesResponse.asUtf8String(), objectInfo)
      }
  }

  override def deleteObject(bucketName: String,
                            key: String,
                            maybeVersionId: Option[String]): Future[(Option[Boolean], Option[String])] = {
    val request = DeleteObjectRequest.builder().bucket(bucketName).key(key).versionId(maybeVersionId.orNull).build()
    s3Client.deleteObject(request).asScala
      .map {
        response =>
          (Option(response.deleteMarker()).map(_.booleanValue()), Option(response.versionId()))
      }
  }

  override def multiPartUpload(bucketName: String, key: String, totalSize: Int): Future[ObjectInfo] = {
    val initiateRequest = CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build()
    for {
      createResponse <- s3Client.createMultipartUpload(initiateRequest).asScala
      uploadId = createResponse.uploadId()
      part1 <- uploadPart(bucketName, key, 1, uploadId)
      part2 <- uploadPart(bucketName, key, 2, uploadId)
      part3 <- uploadPart(bucketName, key, 3, uploadId)
      objectInfo <- completedMultipartUpload(bucketName, key, uploadId, part1 :: part2 :: part3 :: Nil)
    } yield objectInfo
  }

  private def uploadPart(bucketName: String,
                         key: String,
                         partNumber: Int,
                         uploadId: String): Future[CompletedPart] = {
    val uploadRequest = UploadPartRequest.builder().bucket(bucketName).key(key).partNumber(partNumber)
      .uploadId(uploadId).build()
    val filePath = resourcePath -> s"big-sample-$partNumber.txt"
    s3Client.uploadPart(uploadRequest, AsyncRequestBody.fromFile(filePath)).asScala
      .map {
        response => CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
      }
  }

  private def completedMultipartUpload(bucketName: String,
                                       key: String,
                                       uploadId: String,
                                       parts: List[CompletedPart]): Future[ObjectInfo] = {
    val completedMultipartUpload = CompletedMultipartUpload.builder().parts(parts: _*).build()
    val request = CompleteMultipartUploadRequest.builder().bucket(bucketName).key(key).uploadId(uploadId)
      .multipartUpload(completedMultipartUpload).build()
    s3Client.completeMultipartUpload(request).asScala
      .map {
        response =>
          ObjectInfo(
            bucketName = response.bucket(),
            key = response.key(),
            eTag = response.eTag().drop(1).dropRight(1),
            contentMd5 = "",
            contentLength = 0,
            versionId = Option(response.versionId())
          )
      }
  }

}

object AwsClient {
  def apply(awsSettings: AwsSettings)
           (implicit system: ActorSystem[Nothing]): AwsClient = new AwsClient(awsSettings)

  def apply()(implicit system: ActorSystem[Nothing],
              settings: ITSettings): AwsClient = AwsClient(settings.awsSettings)
}
