package com.loyalty.testing.s3

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{ActorSystem => ClassicActorSystem}
import akka.http.scaladsl.server.HttpApp
import akka.util.Timeout
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.routes.Routes
import com.loyalty.testing.s3.service.NotificationService
import com.loyalty.testing.s3.settings.Settings

import scala.concurrent.duration._
import scala.util.Try

class HttpServer(settings: HttpSettings,
                 override protected val objectIO: ObjectIO,
                 override protected val database: NitriteDatabase,
                 override protected val notificationService: NotificationService)
                (override protected implicit val spawnSystem: ActorSystem[Command])
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
  def apply(objectIO: ObjectIO,
            database: NitriteDatabase,
            notificationService: NotificationService)
           (implicit spawnSystem: ActorSystem[Command],
            settings: Settings): HttpServer =
    new HttpServer(settings.http, objectIO, database, notificationService)
}
