package com.loyalty.testing.s3

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sns.{AmazonSNSAsync, AmazonSNSAsyncClientBuilder}
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import com.loyalty.testing.s3.settings.Settings
import com.typesafe.config.Config

class AppSettings(override protected val config: Config) extends Settings {
  def this(system: ActorSystem) = this(system.settings.config)

  @deprecated
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

  @deprecated
  object sqs extends SqsSettings {
    private val builder =
      AmazonSQSAsyncClientBuilder
        .standard()
        .withCredentials(aws.credentialsProvider)

    val sqsClient: AmazonSQSAsync =
      getOptionalEndpointConfiguration("app.sqs.end-point")
        .map(builder.withEndpointConfiguration)
        .getOrElse(builder)
        .build()
  }

  @deprecated
  object sns extends SnsSettings {
    private val builder =
      AmazonSNSAsyncClientBuilder
        .standard()
        .withCredentials(aws.credentialsProvider)

    override val snsClient: AmazonSNSAsync =
      getOptionalEndpointConfiguration("app.sns.end-point")
        .map(builder.withEndpointConfiguration)
        .getOrElse(builder)
        .build()
  }

  private def getOptionalEndpointConfiguration(keyPath: String): Option[EndpointConfiguration] = {
    val endPoint = config.getString(keyPath)
    if (endPoint.isEmpty) None else Some(new EndpointConfiguration(endPoint, aws.region))
  }


}

object AppSettings {
  def apply(config: Config): AppSettings = new AppSettings(config)

  def apply()(implicit system: ActorSystem): AppSettings = new AppSettings(system)
}