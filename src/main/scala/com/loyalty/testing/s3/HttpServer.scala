package com.loyalty.testing.s3

import akka.Done
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.{ActorSystem => ClassicActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.HttpApp
import akka.util.Timeout
import com.loyalty.testing.s3.actor.CopyBehavior.{Command => CopyCommand}
import com.loyalty.testing.s3.actor.NotificationBehavior.{Command => NotificationCommand}
import com.loyalty.testing.s3.actor.model.bucket.{Command => BucketCommand}
import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.routes.Routes
import com.loyalty.testing.s3.settings.Settings

import scala.concurrent.duration._
import scala.util.Try

class HttpServer(settings: HttpSettings,
                 database: NitriteDatabase,
                 override protected val bucketOperationsActorRef: ActorRef[ShardingEnvelope[BucketCommand]],
                 override protected val copyActorRef: ActorRef[ShardingEnvelope[CopyCommand]],
                 override protected val notificationActorRef: ActorRef[ShardingEnvelope[NotificationCommand]])
                (override protected implicit val spawnSystem: ActorSystem[_])
  extends HttpApp
    with Routes {
  override protected implicit val timeout: Timeout = Timeout(10.seconds)

  def start(): Unit = {
    super.startServer(settings.host, settings.port, spawnSystem.toClassic)
  }

  override protected def postServerShutdown(attempt: Try[Done], system: ClassicActorSystem): Unit = {
    super.postServerShutdown(attempt, system)
    Try(database.close()).toOption
    system.terminate()
  }
}

object HttpServer {
  def apply(database: NitriteDatabase,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[BucketCommand]],
            copyActorRef: ActorRef[ShardingEnvelope[CopyCommand]],
            notificationActorRef: ActorRef[ShardingEnvelope[NotificationCommand]])
           (implicit spawnSystem: ActorSystem[_],
            settings: Settings): HttpServer =
    new HttpServer(settings.http, database, bucketOperationsActorRef, copyActorRef, notificationActorRef)
}
