package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import com.loyalty.testing.s3.{data, _}
import com.loyalty.testing.s3.it._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectStatus}
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.{CopyObjectResult, CopyPartResult}
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

  import AwsClient.MinChunkSize
  import system.executionContext

  private val s3Client = S3AsyncClient
    .builder()
    .region(awsSettings.region)
    .credentialsProvider(awsSettings.credentialsProvider)
    .endpointOverride(awsSettings.s3EndPoint.get)
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .httpClient(AkkaHttpClient.builder().withActorSystem(system.toClassic).build())
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

  def listObjects(bucketName: String,
                  delimiter: Option[String],
                  prefix: Option[String],
                  maxKeys: Int = 1000): Future[ListObjectsV2Response] = {
    val request = ListObjectsV2Request.builder().bucket(bucketName).delimiter(delimiter.orNull).prefix(prefix.orNull)
      .maxKeys(maxKeys).build()
    s3Client.listObjectsV2(request).asScala
  }

  override def putObject(bucketName: String,
                         key: String,
                         filePath: Path): Future[data.ObjectInfo] = {
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
          data.ObjectInfo(
            bucketName = bucketName,
            key = key,
            eTag = Option(response.eTag().parseEtag),
            contentLength = contentLength,
            versionId = Option(response.versionId()),
          )
      }
  }

  override def getObject(bucketName: String,
                         key: String,
                         maybeVersionId: Option[String],
                         maybeRange: Option[ByteRange]): Future[(String, data.ObjectInfo)] = {
    val request = GetObjectRequest.builder().bucket(bucketName).key(key).range(getRange(maybeRange))
      .versionId(maybeVersionId.orNull).build()
    s3Client.getObject(request, new ByteArrayAsyncResponseTransformer[GetObjectResponse]()).asScala
      .map {
        bytesResponse =>
          val response = bytesResponse.response()
          val objectInfo = data.ObjectInfo(
            bucketName = bucketName,
            key = key,
            eTag = Option(response.eTag().parseEtag),
            contentLength = bytesResponse.asUtf8String().length,
            versionId = Option(response.versionId())
          )
          (bytesResponse.asUtf8String(), objectInfo)
      }
  }

  def getObjectMeta(bucketName: String,
                    key: String,
                    maybeVersionId: Option[String],
                    maybeRange: Option[ByteRange]): Future[data.ObjectInfo] = {
    val request = HeadObjectRequest.builder().bucket(bucketName).key(key).versionId(maybeVersionId.orNull)
      .range(getRange(maybeRange)).build()
    s3Client.headObject(request).asScala
      .map {
        response =>
          data.ObjectInfo(
            bucketName = bucketName,
            key = key,
            eTag = Option(response.eTag().parseEtag),
            contentLength = response.contentLength(),
            status = ObjectStatus.Active,
            versionId = Option(response.versionId())
          )
      }
  }

  private def getRange(maybeRange: Option[ByteRange]): String = {
    val rangeString =
      maybeRange match {
        case Some(range: Slice) => Option(s"${range.first}-${range.last}")
        case Some(range: FromOffset) => Option(s"${range.offset}-")
        case Some(range: Suffix) => Option(s"-${range.length}")
        case None => None
      }
    rangeString.map(range => s"bytes=$range").orNull
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

  override def multiPartUpload(bucketName: String, key: String, totalSize: Int): Future[data.ObjectInfo] = {
    /*val inputFile = saveFile(1, totalSize, createObjectId(bucketName, key).toString, ".txt")
    val pathAndPartitions = inputFile.map {
      path =>
        val size = Files.size(path)
        createPartitions()(size)
          .map {
            case (partNumber, maybeRange) =>
              RangeDownloadSource()
              (path, partNumber, maybeRange)
          }
    }*/

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
                                       parts: List[CompletedPart]): Future[data.ObjectInfo] = {
    val completedMultipartUpload = CompletedMultipartUpload.builder().parts(parts: _*).build()
    val request = CompleteMultipartUploadRequest.builder().bucket(bucketName).key(key).uploadId(uploadId)
      .multipartUpload(completedMultipartUpload).build()
    s3Client.completeMultipartUpload(request).asScala
      .map {
        response =>
          data.ObjectInfo(
            bucketName = response.bucket(),
            key = response.key(),
            eTag = Option(response.eTag().parseEtag),
            versionId = Option(response.versionId())
          )
      }
  }

  override def copyObject(sourceBucketName: String,
                          sourceKey: String,
                          targetBucketName: String,
                          targetKey: String,
                          maybeSourceVersionId: Option[String]): Future[CopyObjectResult] = {
    val source = createCopySource(sourceBucketName, sourceKey, maybeSourceVersionId)
    val request = CopyObjectRequest.builder().bucket(targetBucketName).key(targetKey).copySource(source).build()
    s3Client.copyObject(request).asScala
      .map {
        response =>
          CopyObjectResult(
            eTag = response.copyObjectResult().eTag().parseEtag,
            maybeSourceVersionId = Option(response.copySourceVersionId()),
            maybeVersionId = Option(response.versionId())
          )
      }
  }

  private def createCopySource(sourceBucketName: String,
                               sourceKey: String,
                               maybeSourceVersionId: Option[String]) = {
    val source = s"/$sourceBucketName/$sourceKey"
    maybeSourceVersionId.map(versionId => s"$source?versionId=$versionId").getOrElse(source)
  }

  override def multiPartCopy(sourceBucketName: String,
                             sourceKey: String,
                             targetBucketName: String,
                             targetKey: String,
                             maybeSourceVersionId: Option[String]): Future[CopyPartResult] = {
    val initiateRequest = CreateMultipartUploadRequest.builder().bucket(targetBucketName).key(targetKey).build()
    val eventualInitiateResponse = s3Client.createMultipartUpload(initiateRequest).asScala
    val eventualPartitions = getObjectMeta(sourceBucketName, sourceKey, maybeSourceVersionId, None).map(_.contentLength)
      .map(createPartitions())
    (for {
      partitions <- eventualPartitions
      initiateResponse <- eventualInitiateResponse
      uploadId = initiateResponse.uploadId()
      completedParts <- copyParts(sourceBucketName, sourceKey, targetBucketName, targetKey, uploadId,
        maybeSourceVersionId, partitions)
      objectInfo <- completedMultipartUpload(targetBucketName, targetKey, uploadId, completedParts)
    } yield objectInfo)
      .map {
        objectInfo => CopyPartResult(objectInfo.eTag.get, objectInfo.versionId)
      }
  }

  private def copyParts(sourceBucketName: String,
                        sourceKey: String,
                        targetBucketName: String,
                        targetKey: String,
                        uploadId: String,
                        maybeSourceVersionId: Option[String],
                        partitions: List[(Int, Option[ByteRange])]): Future[List[CompletedPart]] = {
    val eventualCopyParts =
      partitions
        .map {
          case (partNumber, maybeRange) =>
            copyPart(
              sourceBucketName,
              sourceKey,
              targetBucketName,
              targetKey,
              partNumber,
              uploadId,
              maybeSourceVersionId,
              maybeRange
            )
        }
    Future.sequence(eventualCopyParts)
  }

  private def copyPart(sourceBucketName: String,
                       sourceKey: String,
                       targetBucketName: String,
                       targetKey: String,
                       partNumber: Int,
                       uploadId: String,
                       maybeSourceVersionId: Option[String],
                       maybeRange: Option[ByteRange]) = {
    val source = createCopySource(sourceBucketName, sourceKey, maybeSourceVersionId)
    val range = getRange(maybeRange)
    val request = UploadPartCopyRequest.builder().bucket(targetBucketName).key(targetKey).uploadId(uploadId)
      .partNumber(partNumber).copySource(source).copySourceRange(range).build()
    s3Client.uploadPartCopy(request).asScala
      .map {
        response =>
          CompletedPart.builder().partNumber(partNumber).eTag(response.copyPartResult().eTag()).build()
      }
  }

  private def createPartitions(chunkSize: Int = MinChunkSize)
                              (objectSize: Long): List[(Int, Option[ByteRange])] =
    if (objectSize <= 0 || objectSize < chunkSize) (1, None) :: Nil
    else {
      ((0L until objectSize by chunkSize).toList :+ objectSize)
        .sliding(2)
        .toList
        .zipWithIndex
        .map {
          case (ls, index) =>
            (index + 1, Some(ByteRange(ls.head, ls.last)))
        }
    }

}

object AwsClient {
  private val MinChunkSize = 5 * 1024 * 1024

  def apply(awsSettings: AwsSettings)
           (implicit system: ActorSystem[Nothing]): AwsClient = new AwsClient(awsSettings)

  def apply()(implicit system: ActorSystem[Nothing],
              settings: ITSettings): AwsClient = AwsClient(settings.awsSettings)
}
