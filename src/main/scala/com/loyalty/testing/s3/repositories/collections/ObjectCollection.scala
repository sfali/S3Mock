package com.loyalty.testing.s3.repositories.collections

import java.util.UUID

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.{ObjectMeta, PutObjectResult}
import com.loyalty.testing.s3.utils.DateTimeProvider
import org.dizitart.no2._
import org.dizitart.no2.filters.Filters.{eq => feq, _}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ObjectCollection(db: Nitrite)(implicit dateTimeProvider: DateTimeProvider) {

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
                                         versionIndex: Int): CreateResponse = {
    val maybeVersionId = putObjectResult.maybeVersionId
    val bucketName = bucket.bucketName
    log.info("Request to create object, key={}, bucket={}", Array(key, bucketName): _*)
    val versionEnabled = bucket.version.filter(_ == BucketVersioning.Enabled).getOrElse(BucketVersioning.Suspended) ==
      BucketVersioning.Enabled
    if (versionEnabled && maybeVersionId.isEmpty) {
      throw InvalidInputException(s"Bucket has versioning enabled but no version id provided")
    }

    val objectId = createObjectId(bucketName, key)
    val doc =
      if (versionEnabled) {
        createDocument(IdField, objectId)
          .put(BucketNameField, bucketName)
          .put(KeyField, key)
          .put(VersionIndexField, versionIndex)
          .put(VersionIdField, maybeVersionId.get)
      } else {
        findAllById(objectId) match {
          case Nil =>
            createDocument(IdField, objectId)
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
          CreateResponse(objectId, dateTimeProvider.currentOffsetDateTime)
        }
    }
  }

  def findAll(objectId: UUID): List[ObjectMeta] = findAllById(objectId).map(_.toObjectMeta)

  def findObject(objectId: UUID, maybeVersionId: Option[String] = None): ObjectMeta = {
    //log.info("Finding object, key={}, bucket={}", Array(key, bucketName): _*)
    findById(objectId, maybeVersionId.getOrElse(NonVersionId)) match {
      case Nil => throw NoSuchId(objectId)
      case document :: Nil => document.toObjectMeta
      case _ => throw new IllegalStateException(s"Multiple documents found for $objectId")
    }
  }

  private def findAllById(objectId: UUID): List[Document] =
    collection.find(feq(IdField, objectId.toString), FindOptions.sort(VersionIndexField, SortOrder.Ascending)).toScalaList

  private def findById(objectId: UUID, versionId: String): List[Document] = {
    val filter = and(feq(IdField, objectId.toString), text(versionId, s"*$versionId*"))
    collection.find(filter, FindOptions.sort(VersionIndexField, SortOrder.Ascending)).toScalaList
  }

}

object ObjectCollection {
  def apply(db: Nitrite)(implicit dateTimeProvider: DateTimeProvider): ObjectCollection = new ObjectCollection(db)
}
