package com.loyalty.testing.s3.it.client

import akka.actor.typed.ActorSystem
import com.loyalty.testing.s3.AwsSettings
import com.loyalty.testing.s3.repositories.model.Bucket

import scala.concurrent.Future

class AlpakkaClient(override protected val awsSettings: AwsSettings)
                   (implicit system: ActorSystem[Nothing]) extends S3Client {

  private val awsClient = AwsClient(awsSettings)

  override def createBucket(bucketName: String,
                            region: Option[String]): Future[Bucket] = awsClient.createBucket(bucketName, region)
}
