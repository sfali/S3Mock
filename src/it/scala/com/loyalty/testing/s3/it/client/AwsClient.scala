package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}

import akka.http.scaladsl.model.headers.ByteRange
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._
import com.loyalty.testing.s3.it.{AwsSettings, Settings}

import scala.collection.JavaConverters._

class AwsClient(awsSettings: AwsSettings) {

  import AwsClient._

  private val ChunkSize = 5 * 1024 * 1024

  private val s3Client = {
    val builder = AmazonS3ClientBuilder
      .standard()
      .withCredentials(awsSettings.credentialsProvider)
      .enablePathStyleAccess()

    awsSettings.s3Settings.endPoint
      .map(endPoint => builder.withEndpointConfiguration(endPoint))
      .getOrElse(builder)
      .build()
  }

  def createBucket(bucketName: String, region: Region = Region.US_Standard): Bucket =
    s3Client.createBucket(new CreateBucketRequest(bucketName, region))

  def setBucketVersioning(bucketName: String): Unit = {
    val config = new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)
    val request = new SetBucketVersioningConfigurationRequest(bucketName, config)
    s3Client.setBucketVersioningConfiguration(request)
  }

  def getObject(bucketName: String, key: String, maybeRange: Option[ByteRange.Slice] = None): S3Object = {
    val request =
      maybeRange.map {
        range =>
          new GetObjectRequest(bucketName, key).withRange(range.first, range.last)
      }.getOrElse(new GetObjectRequest(bucketName, key))
    s3Client.getObject(request)
  }

  def getObjectMetadata(bucketName: String, key: String): ObjectMetadata = s3Client.getObjectMetadata(bucketName, key)

  def putObject(bucketName: String, key: String, content: String): PutObjectResult =
    s3Client.putObject(bucketName, key, content)

  def putObject(bucketName: String, key: String, path: Path): PutObjectResult =
    s3Client.putObject(bucketName, key, path.toFile)


  private def initiateMultipartUpload(bucketName: String, key: String): InitiateMultipartUploadResult = {
    val request: InitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, key)
    s3Client.initiateMultipartUpload(request)
  }

  private def uploadMulripart(bucketName: String, key: String, uploadId: String, path: Path)
                             (partition: UploadPartition): UploadPartResult = {
    val request = new UploadPartRequest()
      .withBucketName(bucketName)
      .withKey(key)
      .withPartNumber(partition.partNumber)
      .withUploadId(uploadId)
      .withFile(path.toFile)
      .withFileOffset(partition.filePosition)
      .withPartSize(partition.partSize)
    s3Client.uploadPart(request)
  }

  private def completeMultipartUpload(bucketName: String, key: String,
                                      uploadId: String, parts: List[PartETag]): CompleteMultipartUploadResult = {
    val request = new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts.asJava)
    s3Client.completeMultipartUpload(request)
  }

  def multipartUpload(bucketName: String, key: String, path: Path): CompleteMultipartUploadResult = {
    val initiateResult = initiateMultipartUpload(bucketName, key)

    val partitions = createUploadPartitions(Files.size(path))
    val uploads = partitions
      .map(uploadMulripart(bucketName, key, initiateResult.getUploadId, path))
      .map(_.getPartETag)

    completeMultipartUpload(bucketName, key, initiateResult.getUploadId, uploads)
  }

  private def createUploadPartitions(objectSize: Long): List[UploadPartition] = {
    if (objectSize <= 0) Nil
    else
      ((0L until objectSize by ChunkSize).toList :+ objectSize)
        .sliding(2)
        .toList
        .zipWithIndex
        .map {
          case (ls, index) =>
            UploadPartition(index + 1, ls.head, ls.last - ls.head)
        }
  }


}

object AwsClient {
  def apply(awsSettings: AwsSettings): AwsClient = new AwsClient(awsSettings)

  def apply()(implicit settings: Settings): AwsClient = AwsClient(settings.aws)

  private case class CopyPartition(partNumber: Int, firstByte: Long, lastByte: Long)

  private case class UploadPartition(partNumber: Int, filePosition: Long, partSize: Long)

}
