package com.loyalty.testing.s3.it.client

import java.nio.file.{Files, Path}
import java.util

import akka.http.scaladsl.model.headers.ByteRange
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.loyalty.testing.s3.it.{AwsSettings, Settings}
import com.loyalty.testing.s3.notification.Notification

import scala.jdk.CollectionConverters._

class AwsClient(awsSettings: AwsSettings) {

  import AwsClient._

  private val ChunkSize = 5 * 1024 * 1024

  private val s3Client: AmazonS3 = {
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

  def setBucketNotification(bucketName: String, notification: Notification): Unit = {
    val queueArn = s"arn:aws:sqs:us-eat-1:444455556666:${notification.destinationName}"
    val events = util.EnumSet.of(S3Event.ObjectCreated)
    val queueConfiguration = new QueueConfiguration(queueArn, events)

    (notification.prefix, notification.suffix) match {
      case (None, None) => None

      case (Some(prefix), None) =>
        val filterRule = new FilterRule().withName("prefix").withValue(prefix)
        val filter = new Filter().withS3KeyFilter(new S3KeyFilter().withFilterRules(filterRule))
        queueConfiguration.setFilter(filter)

      case (None, Some(suffix)) =>
        val filterRule = new FilterRule().withName("suffix").withValue(suffix)
        val filter = new Filter().withS3KeyFilter(new S3KeyFilter().withFilterRules(filterRule))
        queueConfiguration.setFilter(filter)

      case (Some(prefix), Some(suffix)) =>
        val prefixFilterRule = new FilterRule().withName("prefix").withValue(prefix)
        val suffixFilterRule = new FilterRule().withName("suffix").withValue(suffix)
        val filter = new Filter().withS3KeyFilter(new S3KeyFilter().withFilterRules(prefixFilterRule, suffixFilterRule))
        queueConfiguration.setFilter(filter)
    }

    val bucketNotificationConfiguration = new BucketNotificationConfiguration()
      .addConfiguration(notification.name, queueConfiguration)

    val bucketNotificationConfigurationRequest = new SetBucketNotificationConfigurationRequest(bucketName, bucketNotificationConfiguration)
    s3Client.setBucketNotificationConfiguration(bucketNotificationConfigurationRequest)
  }

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

  def copyObject(bucketName: String,
                 key: String,
                 sourceBucketName: String,
                 sourceKey: String,
                 maybeSourceVersionId: Option[String] = None): CopyObjectResult = {
    var request = new CopyObjectRequest(sourceBucketName, sourceKey, bucketName, key)
    request = maybeSourceVersionId.map(versionId => request.withSourceVersionId(versionId)).getOrElse(request)
    s3Client.copyObject(request)
  }


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
