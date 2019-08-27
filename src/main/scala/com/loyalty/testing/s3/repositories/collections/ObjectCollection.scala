package com.loyalty.testing.s3.repositories.collections

import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.response.{NoSuchKeyException, ObjectMeta}
import org.dizitart.no2.filters.Filters.{eq => feq, _}
import org.dizitart.no2.{Document, IndexOptions, IndexType, Nitrite}

import scala.collection.JavaConverters._

class ObjectCollection(db: Nitrite) {

  import Document._
  import IndexOptions._
  import IndexType._

  private[repositories] val collection = db.getCollection("objects")
  if (!collection.hasIndex(KeyField)) {
    collection.createIndex(PrefixField, indexOptions(Fulltext))
    collection.createIndex(KeyField, indexOptions(Fulltext))
    collection.createIndex(VersionIdField, indexOptions(Fulltext))
  }

  def createObject(bucket: Bucket, objectMeta: ObjectMeta): ObjectMeta = {
    val bucketName = bucket.bucketName
    val result = objectMeta.result
    val prefix = result.prefix
    val key = result.key
    val doc =
      findById(bucketName, prefix, key) match {
        case Nil =>
          createDocument(BucketNameField, bucketName)
            .put(PrefixField, prefix)
            .put(KeyField, key)
        case document :: Nil => document
        case _ => throw new IllegalStateException(s"Multiple documents found for $bucketName/$prefix/$key")
      }

    val updatedDocument = doc
      .put(ObjectPathField, objectMeta.path.toString)
      .put(ETagField, result.etag)
      .put(ContentMd5Field, result.contentMd5)
      .put(ContentLengthField, result.contentLength)
      .put(VersionIdField, result.maybeVersionId.getOrElse(NonVersionId))
    val docId = collection
      .update(updatedDocument, true)
      .iterator()
      .asScala
      .toList
      .headOption

    if (docId.isEmpty) {
      throw new IllegalStateException(s"unable to get document id for $bucketName/$prefix/$key")
    }

    val lastModifiedTime = collection.getById(docId.get).getLastModifiedTime.toOffsetDateTime.toLocalDateTime
    objectMeta.copy(lastModifiedDate = lastModifiedTime)
  }

  def findObject(bucketName: String, prefix: String, key: String): ObjectMeta = {
    findById(bucketName, prefix, key) match {
      case Nil => throw NoSuchKeyException(bucketName, s"$prefix/$key")
      case document :: Nil => document.toObjectMeta
      case _ => throw new IllegalStateException(s"Multiple documents found for $bucketName/$prefix/$key")
    }
  }

  private def findById(bucketName: String, prefix: String, key: String): List[Document] =
    collection.find(and(feq(BucketNameField, bucketName), feq(PrefixField, prefix), feq(KeyField, key))).toScalaList

}

object ObjectCollection {
  def apply(db: Nitrite): ObjectCollection = new ObjectCollection(db)
}
