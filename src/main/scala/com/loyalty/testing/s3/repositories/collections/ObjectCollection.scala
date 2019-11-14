package com.loyalty.testing.s3.repositories.collections

import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.response.{NoSuchKeyException, ObjectMeta}
import org.dizitart.no2.filters.Filters.{eq => feq, _}
import org.dizitart.no2.{Document, IndexOptions, IndexType, Nitrite}
import org.slf4j.LoggerFactory
import com.loyalty.testing.s3._

import scala.collection.JavaConverters._

class ObjectCollection(db: Nitrite) {

  import Document._
  import IndexOptions._
  import IndexType._

  private val log = LoggerFactory.getLogger(classOf[ObjectCollection])

  private[repositories] val collection = db.getCollection("objects")
  if (!collection.hasIndex(BucketNameField)) {
    collection.createIndex(BucketNameField, indexOptions(Fulltext))
    collection.createIndex(KeyField, indexOptions(Fulltext))
    collection.createIndex(VersionIdField, indexOptions(Fulltext))
  }

  def createObject(bucket: Bucket, objectMeta: ObjectMeta): ObjectMeta = {
    val bucketName = bucket.bucketName
    val result = objectMeta.result
    val key = result.key
    log.info("Request to create object, key={}, bucket={}", Array(key, bucketName): _*)
    val doc =
      findById(bucketName, key) match {
        case Nil =>
          createDocument(ObjectIdField, createObjectId(bucketName, key))
            .put(BucketNameField, bucketName)
            .put(KeyField, key)
        case document :: Nil => document
        case _ => throw new IllegalStateException(s"Multiple documents found for $bucketName/$key")
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

    if (docId.isEmpty)
      throw new IllegalStateException(s"unable to get document id for $bucketName/$key")
    else
      log.info("Object created/updated, key={}, bucket={}, object_path={}, doc_id={}", key, bucketName,
        objectMeta.path.toString, docId.get.getIdValue)

    val lastModifiedTime = collection.getById(docId.get).getLastModifiedTime.toOffsetDateTime.toLocalDateTime
    objectMeta.copy(lastModifiedDate = lastModifiedTime)
  }

  def findObject(bucketName: String,
                 key: String,
                 maybeVersionId: Option[String] = None): ObjectMeta = {
    log.info("Finding object, key={}, bucket={}", Array(key, bucketName): _*)
    findById(bucketName, key, maybeVersionId) match {
      case Nil =>
        log.warn("No object found, key={}, bucket={}", Array(key, bucketName): _*)
        throw NoSuchKeyException(bucketName, s"$key")
      case document :: Nil => document.toObjectMeta
      case _ => throw new IllegalStateException(s"Multiple documents found for $bucketName/$key")
    }
  }

  private def findById(bucketName: String,
                       key: String,
                       maybeVersionId: Option[String] = None): List[Document] = {
    val idFilter = feq(ObjectIdField, createObjectId(bucketName, key))
    val searchFilter = maybeVersionId
      .map(versionId => and(idFilter, text(VersionIdField, s"*$versionId*")))
      .getOrElse(idFilter)
    collection.find(searchFilter).toScalaList
  }

  private def createObjectId(bucketName: String, key: String) = s"$bucketName-$key".toUUID.toString
}

object ObjectCollection {
  def apply(db: Nitrite): ObjectCollection = new ObjectCollection(db)
}
