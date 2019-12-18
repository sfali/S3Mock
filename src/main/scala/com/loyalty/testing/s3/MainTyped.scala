package com.loyalty.testing.s3

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.{ActorSystem => ClassicActorSystem}
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import com.loyalty.testing.s3.actor.{BucketOperationsBehavior, CopyBehavior, NotificationBehavior, ObjectOperationsBehavior}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.service.NotificationService
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.utils.DateTimeProvider
import com.typesafe.config.ConfigFactory

object MainTyped extends App {

  private val config = ConfigFactory.load("application-clustered")

  private sealed trait GuardianRequest

  private final case object InitializeApp extends GuardianRequest

  private val guardian: Behavior[Nothing] =
    Behaviors.setup[GuardianRequest] {
      ctx =>
        implicit val system: ActorSystem[Nothing] = ctx.system
        val classicSystem: ClassicActorSystem = system.toClassic
        implicit val settings: AppSettings = AppSettings(system.settings.config)
        implicit val dateTimeProvider: DateTimeProvider = DateTimeProvider()

        ctx.self ! InitializeApp

        Behaviors.receiveMessage {
          case InitializeApp =>
            system.log.info("Root path of S3: {}", settings.dataDirectory)

            val objectIO = ObjectIO(FileStream())
            val database = NitriteDatabase()
            val notificationService = NotificationService(settings.awsSettings)(classicSystem)

            settings.initialDataPath.foreach(path => initializeInitialData(path, ctx.log, database))

            val sharding: ClusterSharding = ClusterSharding(system)
            val notificationActorRef = NotificationBehavior.init(sharding, database, notificationService)
            val objectOperationsActorRef = ObjectOperationsBehavior.init(sharding, objectIO, database, notificationActorRef)
            val bucketOperationsActorRef = BucketOperationsBehavior.init(sharding, database, objectOperationsActorRef)
            val copyActorRef = CopyBehavior.init(sharding, bucketOperationsActorRef)

            val httpServer = HttpServer(database, bucketOperationsActorRef, copyActorRef, notificationActorRef)
            httpServer.start()

            Behaviors.same
        }
    }.narrow

  ActorSystem[Nothing](guardian, config.getString("app.name"), config)

}
