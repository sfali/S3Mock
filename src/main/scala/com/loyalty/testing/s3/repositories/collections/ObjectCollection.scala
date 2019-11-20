package com.loyalty.testing.s3.repositories.collections

import java.util.UUID

import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.ObjectKey
import com.loyalty.testing.s3.request.BucketVersioning
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

  private[repositories] def createObject(objectKey: ObjectKey): ObjectKey = {
    val bucketName = objectKey.bucketName
    val key = objectKey.key
    log.info("Request to create object, key={}, bucket={}", key, bucketName)
    val version = objectKey.version
    val versionEnabled = version == BucketVersioning.Enabled
    val objectId = objectKey.id
    val doc =
      if (versionEnabled) {
        createDocument(IdField, objectId.toString)
          .put(BucketNameField, bucketName)
          .put(KeyField, key)
          .put(VersionIndexField, objectKey.index)
          .put(VersionField, version.entryName)
          .put(VersionIdField, objectKey.versionId)
      } else {
        findAllById(objectId) match {
          case Nil =>
            createDocument(IdField, objectId.toString)
              .put(BucketNameField, bucketName)
              .put(KeyField, key)
              .put(VersionIndexField, objectKey.index)
              .put(VersionField, version.entryName)
              .put(VersionIdField, objectKey.versionId)
          case document :: _ => document
        }
      }

    val updatedDocument = doc
      .put(ETagField, objectKey.eTag)
      .put(ContentMd5Field, objectKey.contentMd5)
      .put(ContentLengthField, objectKey.contentLength)

    Try(collection.update(updatedDocument, true)) match {
      case Failure(ex) =>
        log.error(s"Error creating/updating document, key=$key, bucket=$bucketName", ex)
        throw DatabaseAccessException(s"Error creating/updating `$key` in the bucket `$bucketName`")
      case Success(writeResult) =>
        val docId = writeResult.iterator().asScala.toList.headOption
        if (docId.isEmpty) throw DatabaseAccessException(s"unable to get document id for $bucketName/$key")
        else {
          log.info("Object created/updated, key={}, bucket={}, doc_id={}", key, bucketName, docId.get.getIdValue)
          objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
        }
    }
  }

  def findAll(objectId: UUID): List[ObjectKey] = findAllById(objectId).map(ObjectKey(_))

  def findObject(objectId: UUID, maybeVersionId: Option[String] = None): ObjectKey = {
    //log.info("Finding object, key={}, bucket={}", Array(key, bucketName): _*)
    findById(objectId, maybeVersionId.getOrElse(NonVersionId)) match {
      case Nil => throw NoSuchId(objectId)
      case document :: Nil => ObjectKey(document)
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
