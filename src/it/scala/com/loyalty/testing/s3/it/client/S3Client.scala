package com.loyalty.testing.s3.it.client

import java.nio.file.Path

import akka.Done
import akka.http.scaladsl.model.headers.ByteRange
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus

import scala.concurrent.Future

trait S3Client {

  protected val awsSettings: AwsSettings

  def createBucket(bucketName: String, region: Option[Region] = None): Future[Bucket]

  def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done]

  def putObject(bucketName: String, key: String, filePath: Path): Future[ObjectKey]

  def getObject(bucketName: String,
                key: String,
                maybeVersionId: Option[String] = None,
                maybeRange: Option[ByteRange] = None): Future[(String, ObjectKey)]
}
