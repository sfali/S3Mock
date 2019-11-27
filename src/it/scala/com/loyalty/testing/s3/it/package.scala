package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}

import com.loyalty.testing.s3.utils.StaticDateTimeProvider

package object it {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()

  private val userDir: String = System.getProperty("user.dir")

  val rootPath: Path = Paths.get(userDir, "target", ".s3mock")

  case class ObjectInfo(bucketName: String,
                        key: String,
                        eTag: String,
                        contentMd5: String,
                        contentLength: Long,
                        versionId: Option[String] = None)

}
