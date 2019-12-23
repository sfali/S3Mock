package com.loyalty.testing.s3.service

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.sns.scaladsl.SnsPublisher
import akka.stream.alpakka.sqs.scaladsl.SqsPublishSink
import akka.stream.scaladsl.Source
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import com.loyalty.testing.s3.AwsSettings
import org.slf4j.LoggerFactory
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest

import scala.concurrent.Future
import scala.jdk.FutureConverters._

class NotificationService(awsSettings: AwsSettings)
                         (implicit system: ActorSystem) {

  import system.dispatcher

  private val log = LoggerFactory.getLogger(classOf[NotificationService])

  private val httpClient: SdkAsyncHttpClient = AkkaHttpClient.builder().withActorSystem(system).build()

  private implicit val awsSqsClient: SqsAsyncClient = {
    val builder =
      SqsAsyncClient
        .builder()
        .credentialsProvider(awsSettings.credentialsProvider)
        .region(awsSettings.region)
        .httpClient(httpClient)
    awsSettings.sqsEndPoint.map(builder.endpointOverride).getOrElse(builder).build()
  }

  private implicit val awsSnsClient: SnsAsyncClient = {
    val builder =
      SnsAsyncClient
        .builder()
        .credentialsProvider(awsSettings.credentialsProvider)
        .region(awsSettings.region)
        .httpClient(httpClient)
    awsSettings.snsEndPoint.map(builder.endpointOverride).getOrElse(builder).build()
  }

  system.registerOnTermination(awsSnsClient.close())
  system.registerOnTermination(awsSqsClient.close())

  def sendSqsMessage(message: String, queueName: String): Future[Done] =
    for {
      queueUrl <- getQueueUrl(queueName)
      _ = log.info("Sending message to queue: {}", queueUrl)
      r <- Source.single(message).runWith(SqsPublishSink(queueUrl))
    } yield r

  def sendSnsMessage(message: String, topicArn: String): Future[Done] =
    Source.single(message).runWith(SnsPublisher.sink(topicArn))

  private def getQueueUrl(queueName: String) = {
    val request = GetQueueUrlRequest.builder().queueName(queueName).build()
    awsSqsClient.getQueueUrl(request).asScala.map(_.queueUrl())
  }
}

object NotificationService {
  def apply(awsSettings: AwsSettings)(implicit system: ActorSystem): NotificationService =
    new NotificationService(awsSettings)
}
