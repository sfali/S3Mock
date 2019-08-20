package com.loyalty.testing.s3

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sns.{AmazonSNSAsync, AmazonSNSAsyncClientBuilder}
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClientBuilder}
import com.typesafe.config.Config

class Settings(config: Config) {
  def this(system: ActorSystem) = this(system.settings.config)

  object http {
    val host: String = config.getString("app.http.host")
    val port: Int = config.getInt("app.http.port")
  }

  val dbSettings: DBSettings = new DBSettings {
    override val filePath: String = config.getString("app.db.file-path")
    override val userName: Option[String] = getOptionalString("app.db-user-name")
    override val password: Option[String] = getOptionalString("app.db-password")
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

  private def getOptionalString(keyPath: String): Option[String] = {
    val maybeValue =
      if (config.hasPath(keyPath)) Some(config.getString(keyPath))
      else None

    maybeValue match {
      case Some(value) => if (value.trim.nonEmpty) Some(value.trim) else None
      case None => None
    }
  }

  private def getOptionalEndpointConfiguration(keyPath: String): Option[EndpointConfiguration] = {
    val endPoint = config.getString(keyPath)
    if (endPoint.isEmpty) None else Some(new EndpointConfiguration(endPoint, aws.region))
  }


}

object Settings {
  def apply(config: Config): Settings = new Settings(config)

  def apply()(implicit system: ActorSystem): Settings = new Settings(system)
}