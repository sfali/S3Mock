package com.loyalty.testing.s3.it.client

import java.nio.file.Path

import akka.Done
import akka.http.scaladsl.model.headers.ByteRange
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.it.ObjectInfo
import com.loyalty.testing.s3.repositories.model.Bucket
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus

import scala.concurrent.Future

trait S3Client {

  protected val awsSettings: AwsSettings

  def createBucket(bucketName: String, region: Option[Region] = None): Future[Bucket]

  def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done]

  def putObject(bucketName: String, key: String, filePath: Path): Future[ObjectInfo]

  def getObject(bucketName: String,
                key: String,
                maybeVersionId: Option[String] = None,
                maybeRange: Option[ByteRange] = None): Future[(String, ObjectInfo)]

  def deleteObject(bucketName: String,
                   key: String,
                   maybeVersionId: Option[String] = None): Future[(Option[Boolean], Option[String])]

  def multiPartUpload(bucketName: String,
                      key: String,
                      totalSize: Int): Future[ObjectInfo]
}
