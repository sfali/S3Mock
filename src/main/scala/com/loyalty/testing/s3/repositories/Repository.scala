package com.loyalty.testing.s3.repositories

import akka.Done
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.request.BucketVersioning.BucketVersioning
import com.loyalty.testing.s3.request.{CompleteMultipartUpload, CreateBucketConfiguration, ListBucketParams, VersioningConfiguration}
import com.loyalty.testing.s3.response._

import scala.concurrent.Future

trait Repository {

  def createBucketWithVersioning(bucketName: String, bucketConfiguration: CreateBucketConfiguration,
                                 maybeBucketVersioning: Option[BucketVersioning]): Future[BucketResponse]

  def createBucket(bucketName: String, bucketConfiguration: CreateBucketConfiguration): Future[BucketResponse]

  def setBucketVersioning(bucketName: String, contentSource: Source[ByteString, _]): Future[BucketResponse]

  def setBucketVersioning(bucketName: String, versioningConfiguration: VersioningConfiguration): Future[BucketResponse]

  def setBucketNotification(bucketName: String, contentSource: Source[ByteString, _]): Future[Done]

  def setBucketNotification(bucketName: String, notifications: List[Notification]): Future[Done]

  def listBucket(bucketName: String, params: ListBucketParams): Future[ListBucketResult]

  def putObject(bucketName: String, key: String, contentSource: Source[ByteString, _]): Future[ObjectMeta]

  def getObject(bucketName: String, key: String, maybeVersionId: Option[String] = None,
                maybeRange: Option[ByteRange] = None): Future[GetObjectResponse]

  def deleteObject(bucketName: String, key: String, maybeVersionId: Option[String] = None): Future[DeleteObjectResponse.type]

  def initiateMultipartUpload(bucketName: String, key: String): Future[InitiateMultipartUploadResult]

  def uploadMultipart(bucketName: String, key: String, partNumber: Int, uploadId: String,
                      contentSource: Source[ByteString, _]): Future[ObjectMeta]

  def copyObject(bucketName: String,
                 key: String,
                 sourceBucketName: String,
                 sourceKey: String,
                 maybeSourceVersionId: Option[String] = None): Future[(ObjectMeta, CopyObjectResult)]

  def copyMultipart(bucketName: String,
                    key: String,
                    partNumber: Int,
                    uploadId: String,
                    sourceBucketName: String,
                    sourceKey: String,
                    maybeSourceVersionId: Option[String] = None,
                    maybeSourceRange: Option[ByteRange] = None): Future[CopyPartResult]

  def completeMultipart(bucketName: String, key: String, uploadId: String,
                        completeMultipartUpload: CompleteMultipartUpload): Future[CompleteMultipartUploadResult]

  def clean(): Unit
}
