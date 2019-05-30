package com.loyalty.testing.s3

import java.nio.file.{Files, Paths}

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.HttpApp
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer}
import com.loyalty.testing.s3.data.BootstrapConfiguration
import com.loyalty.testing.s3.notification.actor.NotificationRouter
import com.loyalty.testing.s3.repositories.{FileRepository, FileStore}
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration}
import com.loyalty.testing.s3.routes.S3Routes
import com.typesafe.config.ConfigFactory
import io.circe.generic.auto._
import io.circe.parser._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object Main extends HttpApp with App with S3Routes {

  private val config = ConfigFactory.load()
  private implicit val system: ActorSystem = ActorSystem(config.getString("app.name"), config)
  override protected implicit val materializer: Materializer = ActorMaterializer(ActorMaterializerSettings(system))
  private implicit val settings: Settings = Settings()

  import system.dispatcher

  override implicit protected val log: LoggingAdapter = system.log
  private val root = Paths.get(System.getProperty("user.dir"), ".s3mock")
  log.info("Root path of S3: {}", root.toAbsolutePath)
  private val fileStore: FileStore = FileStore(root)
  override implicit protected val repository: FileRepository = FileRepository(fileStore, log)

  initializeInitialData()

  override protected val notificationRouter: ActorRef = system.actorOf(
    NotificationRouter.props(fileStore),
    "notification-router")

  override protected def routes = s3Routes

  override protected def postServerShutdown(attempt: Try[Done], system: ActorSystem): Unit = {
    super.postServerShutdown(attempt, system)
    repository.clean()
    system.terminate()
  }

  private def initializeInitialData(): Unit = {
    val dataPath = Paths.get(System.getProperty("user.dir"), "s3", "initial.json")
    log.info("DataPath: {}", dataPath)
    if (Files.exists(dataPath)) {
      log.info("Data found @: {}", dataPath)
      val json = Files.readAllLines(dataPath).asScala.mkString(System.lineSeparator())
      val result = decode[BootstrapConfiguration](json)
      result match {
        case Left(error) =>
          log.warning("Unable to parse Json: {}, error message: {}:{}", json,
            error.getClass.getName, error.getMessage)
        case Right(bootstrapConfiguration) =>
          initializeBuckets(bootstrapConfiguration)
      }
    } else {
      log.info("No initial data found at: {}", dataPath)
    }
  }

  private def initializeBuckets(bootstrapConfiguration: BootstrapConfiguration): Unit =
    bootstrapConfiguration.initialBuckets
      .foreach {
        initialBucket =>
          val maybeBucketVersioning =
            if (initialBucket.enableVersioning)
              Some(BucketVersioning.Enabled)
            else None

          val bucketName = initialBucket.bucketName
          val notifications = bootstrapConfiguration.notifications.filter(_.bucketName == bucketName)

          (for {
            bucketResponse <- repository.createBucketWithVersioning(bucketName, CreateBucketConfiguration(),
              maybeBucketVersioning)
            _ <- repository.setBucketNotification(bucketName, notifications)
          } yield bucketResponse)
            .onComplete {
              case Success(bucketResponse) =>
                log.info("Bucket created: {}", bucketResponse.bucketName)
              case Failure(ex) => log.warning("Failed to create bucket: {}, message {}", bucketName, ex.getMessage)
            }
      }

  Main.startServer(settings.http.host, settings.http.port, system)
}
