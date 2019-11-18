package com.loyalty.testing.s3.repositories.collections

import java.util.UUID

import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response._
import org.dizitart.no2.IndexOptions.indexOptions
import org.dizitart.no2.IndexType.Unique
import org.dizitart.no2.filters.Filters.{eq => feq}
import org.dizitart.no2.{Document, Nitrite}

class BucketCollection(db: Nitrite) {

  import Document._

  private[repositories] val collection = db.getCollection("bucket")
  if (!collection.hasIndex(BucketNameField)) {
    collection.createIndex(BucketNameField, indexOptions(Unique))
  }

  private[repositories] def createBucket(bucket: Bucket): Bucket =
    findById(bucket.id) match {
      case Nil =>
        val document =
          createDocument(IdField, bucket.id.toString)
            .put(BucketNameField, bucket.bucketName)
            .put(RegionField, bucket.region)
            .put(VersionField, bucket.version.orNull)
        collection.insert(document)
        bucket
      case _ => throw BucketAlreadyExistsException(bucket.bucketName)
    }

  private[repositories] def setBucketVersioning(bucketId: UUID, bucketVersioning: BucketVersioning): Bucket =
    findById(bucketId) match {
      case Nil => throw NoSuchBucketException(bucketId.toString)
      case document :: Nil =>
        val updatedDocument = document.put(VersionField, bucketVersioning.entryName)
        collection.update(updatedDocument)
        Bucket(updatedDocument)
      case _ => throw new IllegalStateException(s"More than one document found: $bucketId")
    }

  private[repositories] def findBucket(id: UUID): Bucket =
    findById(id) match {
      case Nil => throw NoSuchBucketException(id.toString)
      case document :: Nil => Bucket(document)
      case _ => throw new IllegalStateException(s"More than one document found: $id")
    }

  private[repositories] def findBucket(bucketName: String): Bucket =
    findByName(bucketName) match {
      case Nil => throw NoSuchBucketException(bucketName)
      case document :: Nil => Bucket(document)
      case _ => throw new IllegalStateException(s"More than one document found: $bucketName")
    }

  private def findById(id: UUID): List[Document] =
    collection.find(feq(IdField, id.toString)).toScalaList

  private def findByName(bucketName: String): List[Document] =
    collection.find(feq(BucketNameField, bucketName)).toScalaList
}

object BucketCollection {
  def apply(db: Nitrite): BucketCollection = new BucketCollection(db)
}
