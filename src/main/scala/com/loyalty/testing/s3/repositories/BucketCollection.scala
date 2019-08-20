package com.loyalty.testing.s3.repositories

import java.nio.file.Path

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.response._
import org.dizitart.no2.filters.Filters.{eq => feq}
import org.dizitart.no2.{Document, Nitrite}

class BucketCollection(db: Nitrite, dataDir: Path) {

  import Document._

  private[repositories] val collection = db.getCollection("bucket")

  def createBucket(bucketName: String,
                   region: String,
                   version: Option[String] = None): Bucket =
    findByName(bucketName) match {
      case Nil =>
        val document =
          createDocument(BucketNameField, bucketName)
            .put(RegionField, region)
            .put(BucketPathField, (dataDir + bucketName).toString)
            .put(VersionField, version.orNull)
        collection.insert(document)
        Bucket(document)
      case _ => throw BucketAlreadyExistsException(bucketName)
    }

  def setBucketVersioning(bucketName: String, version: String): Bucket =
    findByName(bucketName) match {
      case Nil => throw NoSuchBucketException(bucketName)
      case document :: Nil =>
        val doc = document.put(VersionField, version)
        collection.update(doc)
        Bucket(document)
      case _ => throw new IllegalStateException(s"More than one document found: $bucketName")
    }

  private def findByName(bucketName: String): List[Document] =
    collection.find(feq(BucketNameField, bucketName)).toScalaList
}

object BucketCollection {
  def apply(db: Nitrite, dataDir: Path): BucketCollection = new BucketCollection(db, dataDir)
}
