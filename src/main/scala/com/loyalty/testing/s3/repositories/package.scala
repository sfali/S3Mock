package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}
import java.util

import org.dizitart.no2.{Cursor, Document}

import scala.collection.JavaConverters._

package object repositories {

  val BucketNameField = "bucket-name"
  val RegionField = "region"
  val BucketPathField = "bucket-path"
  val VersionField = "version"

  implicit class CursorOps(src: Cursor) {
    def toScalaList: List[Document] = src.asScala.toList
  }

  implicit class DocumentOps(src: Document) {
    def getString(key: String): String = src.get(key, classOf[String])

    def getBoolean(key: String): Boolean = src.get(key, classOf[Boolean])

    def getOptionalString(key: String): Option[String] = Option(getString(key))

    def getPath(key: String): Path = Paths.get(getString(key))

    def getOptionalForeignField(key: String): Option[List[Document]] =
      Option(src.get(key, classOf[util.HashSet[Document]])).map(_.asScala.toList)
  }

}
