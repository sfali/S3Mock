package com.loyalty.testing.s3.it.client

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.Bucket

import scala.concurrent.Future

trait S3Client {

  protected val awsSettings: AwsSettings

  def createBucket(bucketName: String, region: Option[String] = None): Future[Bucket]
}
