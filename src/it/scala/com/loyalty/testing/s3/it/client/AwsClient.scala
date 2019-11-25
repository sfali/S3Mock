package com.loyalty.testing.s3.it.client

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.it.ITSettings
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.BucketAlreadyExistsException
import software.amazon.awssdk.services.s3.model.{CreateBucketConfiguration, CreateBucketRequest, BucketAlreadyExistsException => AwsBucketAlreadyExistsException}
import software.amazon.awssdk.services.s3.{S3Configuration, S3Client => AwsS3Client}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AwsClient(override protected val awsSettings: AwsSettings) extends S3Client {

  private val s3Client = AwsS3Client
    .builder()
    .region(awsSettings.region)
    .credentialsProvider(awsSettings.credentialsProvider)
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .build()

  override def createBucket(bucketName: String, region: Option[String]): Future[Bucket] = {
    val requestBuilder = CreateBucketRequest.builder().bucket(bucketName)
    val request =
      region match {
        case Some(value) =>
          val c = CreateBucketConfiguration.builder().locationConstraint(value).build()
          requestBuilder.createBucketConfiguration(c).build()
        case None => requestBuilder.build()
      }
    Try(s3Client.createBucket(request)) match {
      case Failure(_: AwsBucketAlreadyExistsException) => Future.failed(BucketAlreadyExistsException(bucketName))
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Bucket(bucketName, region.getOrElse(defaultRegion),
        BucketVersioning.NotExists))
    }
  }
}

object AwsClient {
  def apply(awsSettings: AwsSettings): AwsClient = new AwsClient(awsSettings)

  def apply()(implicit settings: ITSettings): AwsClient = AwsClient(settings.awsSettings)
}
