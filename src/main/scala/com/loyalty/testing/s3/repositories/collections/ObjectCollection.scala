package com.loyalty.testing.s3.repositories.collections

import java.time.OffsetDateTime

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.{NoSuchKeyException, ObjectMeta, PutObjectResult}
import org.dizitart.no2._
import org.dizitart.no2.filters.Filters.{eq => feq}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

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

  private[repositories] def createObject(bucket: Bucket,
                                         key: String,
                                         putObjectResult: PutObjectResult,
                                         versionIndex: Int,
                                         maybeVersionId: Option[String]): CreateResponse = {
    val bucketName = bucket.bucketName
    log.info("Request to create object, key={}, bucket={}", Array(key, bucketName): _*)
    val versionEnabled = bucket.version.filter(_ == BucketVersioning.Enabled).getOrElse(BucketVersioning.Suspended) ==
      BucketVersioning.Enabled
    if (versionEnabled && maybeVersionId.isEmpty) {
      throw InvalidInputException(s"Bucket has versioning enabled but no version id provided")
    }

    val doc =
      if (versionEnabled) {
        createDocument(IdField, createObjectId(bucketName, key))
          .put(BucketNameField, bucketName)
          .put(KeyField, key)
          .put(VersionIndexField, versionIndex)
          .put(VersionIdField, maybeVersionId.get)
      } else {
        findById(bucketName, key) match {
          case Nil =>
            createDocument(IdField, createObjectId(bucketName, key))
              .put(BucketNameField, bucketName)
              .put(KeyField, key)
              .put(VersionIndexField, 0)
              .put(VersionIdField, NonVersionId)
          case document :: _ => document
        }
      }

    val updatedDocument = doc
      .put(ETagField, putObjectResult.etag)
      .put(ContentMd5Field, putObjectResult.contentMd5)
      .put(ContentLengthField, putObjectResult.contentLength)

    Try(collection.update(updatedDocument, true)) match {
      case Failure(ex) =>
        log.error(s"Error creating/updating document, key=$key, bucket=$bucketName", ex)
        throw DatabaseAccessException(s"Error creating/updating `$key` in the bucket `$bucketName`")
      case Success(writeResult) =>
        val docId = writeResult.iterator().asScala.toList.headOption
        if (docId.isEmpty) throw DatabaseAccessException(s"unable to get document id for $bucketName/$key")
        else {
          log.info("Object created/updated, key={}, bucket={}, doc_id={}", key, bucketName, docId.get.getIdValue)
          CreateResponse(docId.get.getIdValue, OffsetDateTime.now())
        }
    }
  }

  @deprecated
  def createObject(bucket: Bucket, objectMeta: ObjectMeta): ObjectMeta = {
    val bucketName = bucket.bucketName
    val result = objectMeta.result
    val key = result.key
    log.info("Request to create object, key={}, bucket={}", Array(key, bucketName): _*)
    val doc =
      findById(bucketName, key) match {
        case Nil =>
          createDocument(IdField, createObjectId(bucketName, key))
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

  def findAll(bucketName: String, key: String): List[Document] = findById(bucketName, key)

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
    collection.find(feq(IdField, createObjectId(bucketName, key)),
      FindOptions.sort(VersionIndexField, SortOrder.Ascending)).toScalaList
  }

  private def createObjectId(bucketName: String, key: String) = s"$bucketName-$key".toUUID.toString
}

object ObjectCollection {
  def apply(db: Nitrite): ObjectCollection = new ObjectCollection(db)
}
