package com.loyalty.testing.s3.settings

import java.net.URI
import java.nio.file.{Files, Path}

import com.loyalty.testing.s3._
import com.typesafe.config.Config
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region

trait Settings {

  protected val config: Config

  val http: HttpSettings = HttpSettings(config)

  val enableNotification: Boolean = config.getBoolean("app.enable-notification")

  val dataDirectory: Path =
    config.getOptionalString("app.bootstrap.data-directory") match {
      case Some(path) => path.toPath
      case None => UserDir -> ".s3mock"
    }

  val initialDataPath: Option[Path] = {
    val path =
      config.getOptionalString("app.bootstrap.initial-data-file") match {
        case Some(fileName) => fileName.toPath
        case None => dataDirectory -> "initial.json"
      }
    Some(path).filter(Files.exists(_))
  }


  val awsSettings: AwsSettings = new AwsSettings {
    override val region: Region = Region.of(config.getString("app.aws.region"))
    override val credentialsProvider: AwsCredentialsProvider = {
      val credProviderPath = "app.aws.credentials.provider"
      config.getString(credProviderPath) match {
        case "default" => DefaultCredentialsProvider.create()
        case "anon" => AnonymousCredentialsProvider.create()
        case "static" =>
          val aki = config.getString("app.aws.credentials.access-key-id")
          val sak = config.getString("app.aws.credentials.secret-access-key")
          val tokenPath = "app.aws.credentials.token"
          val creds = if (config.hasPath(tokenPath)) {
            AwsSessionCredentials.create(aki, sak, config.getString(tokenPath))
          } else {
            AwsBasicCredentials.create(aki, sak)
          }
          StaticCredentialsProvider.create(creds)
        case _ => DefaultCredentialsProvider.create()
      }
    }
    override val sqsEndPoint: Option[URI] = config.getOptionalUri("app.aws.sqs.end-point")
    override val s3EndPoint: Option[URI] = config.getOptionalUri("app.aws.s3.end-point")
    override val snsEndPoint: Option[URI] = config.getOptionalUri("app.aws.sns.end-point")
  }

  val dbSettings: DBSettings = DBSettings(config)

}
