package com.loyalty.testing.s3

import java.nio.file.Paths

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.HttpApp
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.loyalty.testing.s3.notification.actor.NotificationRouter
import com.loyalty.testing.s3.repositories.{FileRepository, FileStore}
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration}
import com.loyalty.testing.s3.routes.S3Routes
import com.typesafe.config.ConfigFactory

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

  private val initialBuckets: List[String] = settings.bootstrap.initialBuckets
  private val versionedBuckets: List[String] = settings.bootstrap.versionedBuckets

  private val notificationRouter = system.actorOf(NotificationRouter.props(), "notification-router")

  initialBuckets
    .foreach {
      bucketName =>
        val maybeBucketVersioning = versionedBuckets.find(_ == bucketName).flatMap(_ => Some(BucketVersioning.Enabled))
        repository.createBucketWithVersioning(bucketName.trim, CreateBucketConfiguration(), maybeBucketVersioning)
          .onComplete {
            case Success(bucketResponse) =>
              log.info("Bucket created: {}", bucketResponse.bucketName)
            case Failure(ex) => log.warning("Failed to create bucket: {}, message {}", ex.getMessage)
          }
    }

  override protected def routes = s3Routes

  override protected def postServerShutdown(attempt: Try[Done], system: ActorSystem): Unit = {
    super.postServerShutdown(attempt, system)
    repository.clean()
    system.terminate()
  }

  Main.startServer(settings.http.host, settings.http.port, system)
}
