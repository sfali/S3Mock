package com.loyalty.testing.s3.it.client

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.Bucket
import software.amazon.awssdk.regions.Region

import scala.concurrent.Future

trait S3Client {

  protected val awsSettings: AwsSettings

  def createBucket(bucketName: String, region: Option[Region] = None): Future[Bucket]
}
