package com.loyalty.testing.s3

import java.nio.file.{Files, Paths}

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.HttpApp
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.loyalty.testing.s3.data.BootstrapConfiguration
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.notification.actor.NotificationRouter
import com.loyalty.testing.s3.repositories.{FileRepository, FileStore}
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration}
import com.loyalty.testing.s3.routes.S3Routes
import com.typesafe.config.ConfigFactory
import io.circe.parser.decode

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object Main extends HttpApp with App with S3Routes {

  private val config = ConfigFactory.load()
  private implicit val system: ActorSystem = ActorSystem(config.getString("app.name"), config)
  override protected implicit val mat: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))
  private implicit val settings: Settings = Settings()

  import system.dispatcher

  override implicit protected val log: LoggingAdapter = system.log
  private val root = Paths.get(System.getProperty("user.dir"), ".s3mock")
  log.info("Root path of S3: {}", root.toAbsolutePath)
  override implicit protected val repository: FileRepository = FileRepository(FileStore(root), log)

  private val notifications: List[Notification] =
    settings.bootstrap.dataPath match {
      case Some(path) =>
        val json = Files.readAllLines(path).asScala.mkString(System.lineSeparator())
        val result = decode[BootstrapConfiguration](json)
        result match {
          case Left(error) =>
            log.warning("Unable to parse Json: {}, errorr message: {}:{}", json,
              error.getClass.getName, error.getMessage)
            Nil
          case Right(bootstrapConfiguration) =>
            initializeBuckets(bootstrapConfiguration)
            bootstrapConfiguration.notifications
        }
      case None => Nil
    }

  private val notificationRouter = system.actorOf(NotificationRouter.props(notifications), "notification-router")

  override protected def routes = s3Routes

  override protected def postServerShutdown(attempt: Try[Done], system: ActorSystem): Unit = {
    super.postServerShutdown(attempt, system)
    repository.clean()
    system.terminate()
  }

  private def initializeBuckets(bootstrapConfiguration: BootstrapConfiguration): Unit =
    bootstrapConfiguration.initialBuckets
      .foreach {
        initialBucket =>
          val maybeBucketVersioning =
            if (initialBucket.enableVersioning)
              Some(BucketVersioning.Enabled)
            else None

          repository.createBucketWithVersioning(initialBucket.bucketName, CreateBucketConfiguration(),
            maybeBucketVersioning)
            .onComplete {
              case Success(bucketResponse) =>
                log.info("Bucket created: {}", bucketResponse.bucketName)
              case Failure(ex) => log.warning("Failed to create bucket: {}, message {}", ex.getMessage)
            }
      }

  Main.startServer(settings.http.host, settings.http.port, system)
}
