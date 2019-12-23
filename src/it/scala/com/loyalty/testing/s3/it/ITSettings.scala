package com.loyalty.testing.s3.it

import com.loyalty.testing.s3.settings.Settings
import com.typesafe.config.Config

class ITSettings(override protected val config: Config) extends Settings {

}

object ITSettings {
  def apply(config: Config): ITSettings = new ITSettings(config)
}
