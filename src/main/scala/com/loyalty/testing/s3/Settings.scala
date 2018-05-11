package com.loyalty.testing.s3

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.util.Try

class Settings(config: Config) {
  def this(system: ActorSystem) = this(system.settings.config)

  object http {
    val host: String = config.getString("app.http.host")
    val port: Int = config.getInt("app.http.port")
  }

  object bootstrap {
    val initialBuckets: List[String] = initializeList("app.bootstrap.initial-buckets")
    val versionedBuckets: List[String] = initializeList("app.bootstrap.versioned-buckets")
  }

  private def initializeList(path: String): List[String] = {
    val maybeValue = Option(config.getString(path))
    if (maybeValue.getOrElse("").trim.nonEmpty)
      Try(maybeValue.get.split(",")).toOption.map(_.toList).getOrElse(Nil)
    else Nil
  }

}

object Settings {
  def apply(config: Config): Settings = new Settings(config)

  def apply()(implicit system: ActorSystem): Settings = new Settings(system)
}