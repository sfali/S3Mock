package com.loyalty.testing.s3.it

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.typesafe.config.{Config, ConfigFactory}

class Settings private(config: Config) {

  object aws extends AwsSettings {
    override val region: String = config.getString("app.aws.region")
    override val credentialsProvider: AWSCredentialsProvider = {
      val credProviderPath = "app.aws.credentials.provider"
      config.getString(credProviderPath) match {
        case "default" => DefaultAWSCredentialsProviderChain.getInstance()
        case "static" => new ProfileCredentialsProvider("assumed_role")
        case "anon" => new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())
        case _ => DefaultAWSCredentialsProviderChain.getInstance()
      }
    }

    override object s3Settings extends S3Settings {
      override val endPoint: Option[EndpointConfiguration] = {
        val endPoint = config.getString("app.s3.end-point")
        if (endPoint.isEmpty) None else Some(new EndpointConfiguration(endPoint, region))
      }
    }

  }

}

object Settings {
  def apply(config: Config): Settings = new Settings(config)

  def apply()(implicit system: ActorSystem): Settings = Settings(system.settings.config)

  def apply(resourcePath: String): Settings = Settings(ConfigFactory.load(resourcePath))
}
