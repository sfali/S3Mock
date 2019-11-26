package com.loyalty.testing.s3.it.client

import akka.Done
import akka.actor.typed.ActorSystem
import com.loyalty.testing.s3.AwsSettings
import com.loyalty.testing.s3.it.ITSettings
import com.loyalty.testing.s3.repositories.model.Bucket
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus

import scala.concurrent.Future

class AlpakkaClient(override protected val awsSettings: AwsSettings)
                   (implicit system: ActorSystem[Nothing]) extends S3Client {

  private val awsClient = AwsClient(awsSettings)

  override def createBucket(bucketName: String, region: Option[Region]): Future[Bucket] =
    awsClient.createBucket(bucketName, region)

  override def setBucketVersioning(bucketName: String, status: BucketVersioningStatus): Future[Done] =
    awsClient.setBucketVersioning(bucketName, status)
}

object AlpakkaClient {
  def apply()(implicit system: ActorSystem[Nothing],
              settings: ITSettings): AlpakkaClient =
    new AlpakkaClient(settings.awsSettings)
}
