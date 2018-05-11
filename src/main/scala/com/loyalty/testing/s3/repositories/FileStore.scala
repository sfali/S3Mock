package com.loyalty.testing.s3.repositories

import java.nio.file.{FileVisitOption, Files, Path}
import java.util.Comparator

import scala.collection.mutable

class FileStore(root: Path) {

  import com.loyalty.testing.s3._

  val workDir: Path = root.toAbsolutePath
  val dataDir: Path = workDir + "data"
  val uploadsDir: Path = workDir + "uploads"

  private val bucketMetadataMap: mutable.Map[String, BucketMetadata] = mutable.Map()

  def add(bucketName: String, metadata: BucketMetadata): Unit = bucketMetadataMap += (bucketName -> metadata)

  def remove(bucketName: String): Unit = bucketMetadataMap -= bucketName

  def get(bucketName: String): Option[BucketMetadata] = bucketMetadataMap.get(bucketName)

  def removeObject(bucketName: String, key: String): Unit =
    bucketMetadataMap.get(bucketName).fold(())(_.removeMetadata(key))

  def getOrElse(bucketName: String, default: => BucketMetadata): BucketMetadata =
    bucketMetadataMap.getOrElse(bucketName, default)

  def clean(path: Path): Boolean = {
    Files.walk(path, FileVisitOption.FOLLOW_LINKS)
      .sorted(Comparator.reverseOrder())
      .forEach((path: Path) => path.toFile.delete())
    Files.deleteIfExists(path)
  }

  def clean: Boolean = clean(workDir)

  def print(): Unit = {
    bucketMetadataMap.foreach {
      mapValue =>
        println(mapValue._1)
        println(mapValue._2.toString)
    }
  }
}

object FileStore {
  def apply(root: Path): FileStore = new FileStore(root)
}
