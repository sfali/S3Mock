package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}

package object it {

  private val userDir: String = System.getProperty("user.dir")

  val rootPath: Path = Paths.get(userDir, "target", ".s3mock")

}
