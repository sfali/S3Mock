package com.loyalty.testing.s3

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sns.{AmazonSNSAsync, AmazonSNSAsyncClientBuilder}
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import com.typesafe.config.Config

import scala.util.Try

class Settings(config: Config) {
  def this(system: ActorSystem) = this(system.settings.config)

  object http {
    val host: String = config.getString("app.http.host")
    val port: Int = config.getInt("app.http.port")
  }

  object bootstrap {
    val initialBuckets: List[String] = initializeList("app.bootstrap.initial-buckets")
    val versionedBuckets: List[String] = initializeList("app.bootstrap.versioned-buckets")
  }

  object aws {
    val region: String = config.getString("app.aws.region")
    val credentialsProvider: AWSCredentialsProvider = {
      val credProviderPath = "app.aws.credentials.provider"
      config.getString(credProviderPath) match {
        case "default" => DefaultAWSCredentialsProviderChain.getInstance()
        case "anon" => new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())
        case "static" =>
          val aki = config.getString("aws.credentials.access-key-id")
          val sak = config.getString("aws.credentials.secret-access-key")
          val tokenPath = "aws.credentials.token"
          val creds = if (config.hasPath(tokenPath)) {
            new BasicSessionCredentials(aki, sak, config.getString(tokenPath))
          } else {
            new BasicAWSCredentials(aki, sak)
          }
          new AWSStaticCredentialsProvider(creds)
        case _ => DefaultAWSCredentialsProviderChain.getInstance()
      }
    }
  }

  object sqs {
    private val endPoint: Option[EndpointConfiguration] = {
      val endPoint = config.getString("app.sqs.end-point")
      if (endPoint.isEmpty) None else Some(new EndpointConfiguration(endPoint, aws.region))
    }
    val queueUrl: String = config.getString("app.sqs.queue-url")
    private val builder =
      AmazonSQSAsyncClientBuilder
        .standard()
        .withCredentials(aws.credentialsProvider)
    val sqsClient: AmazonSQSAsync =
      endPoint
        .map(ep => builder.withEndpointConfiguration(ep))
        .getOrElse(builder)
        .build()
  }

  object sns {
    private val endPoint: Option[EndpointConfiguration] = {
      val endPoint = config.getString("app.sns.end-point")
      if (endPoint.isEmpty) None else Some(new EndpointConfiguration(endPoint, aws.region))
    }
    val topicArn: String = config.getString("app.sns.topic-arn")
    private val builder =
      AmazonSNSAsyncClientBuilder
        .standard()
        .withCredentials(aws.credentialsProvider)
    val snsClient: AmazonSNSAsync =
      endPoint
        .map(ep => builder.withEndpointConfiguration(ep))
        .getOrElse(builder)
        .build()
  }

  private def initializeList(path: String): List[String] = {
    val maybeValue = Option(config.getString(path))
    if (maybeValue.getOrElse("").trim.nonEmpty)
      Try(maybeValue.get.split(",")).toOption.map(_.toList).getOrElse(Nil)
    else Nil
  }

}

object Settings {
  def apply(config: Config): Settings = new Settings(config)

  def apply()(implicit system: ActorSystem): Settings = new Settings(system)
}