package com.loyalty.testing.s3

import java.nio.file.Paths

// import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.ActorSystem
import akka.util.Timeout
import com.loyalty.testing.s3.actor.SpawnBehavior
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.utils.DateTimeProvider
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object MainTyped extends App {

  private val config = ConfigFactory.load()
  private implicit val system: ActorSystem[SpawnBehavior.Command] = ActorSystem(SpawnBehavior(),
    config.getString("app.name"))
  private implicit val dateTimeProvider: DateTimeProvider = DateTimeProvider()
  private implicit val settings: AppSettings = AppSettings(system.settings.config)
  implicit val timeout: Timeout = Timeout(3.seconds)

  private val root = Paths.get(System.getProperty("user.dir"), ".s3mock")
  system.log.info("Root path of S3: {}", root.toAbsolutePath)

  private val objectIO = ObjectIO(root, FileStream())
  private val database = NitriteDatabase(root, settings.dbSettings)

}
