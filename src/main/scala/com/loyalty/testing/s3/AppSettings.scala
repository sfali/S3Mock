package com.loyalty.testing.s3

import akka.actor.ActorSystem
import com.loyalty.testing.s3.settings.Settings
import com.typesafe.config.Config

class AppSettings(override protected val config: Config) extends Settings {
  def this(system: ActorSystem) = this(system.settings.config)
}

object AppSettings {
  def apply(config: Config): AppSettings = new AppSettings(config)

  def apply()(implicit system: ActorSystem): AppSettings = new AppSettings(system)
}