package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}

import com.loyalty.testing.s3.utils.StaticDateTimeProvider

package object it {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()

  private val userDir: String = System.getProperty("user.dir")

  val rootPath: Path = Paths.get(userDir, "target", ".s3mock")

}
