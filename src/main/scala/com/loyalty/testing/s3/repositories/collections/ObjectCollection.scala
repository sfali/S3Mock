package com.loyalty.testing.s3.repositories.collections

import java.util.UUID

import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.{ObjectKey, ObjectStatus}
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
    collection.createIndex(BucketNameField, indexOptions(NonUnique))
    collection.createIndex(KeyField, indexOptions(Fulltext))
    collection.createIndex(VersionIdField, indexOptions(NonUnique))
  }

  private[repositories] def createOrUpdateObject(objectKey: ObjectKey): ObjectKey = {
    val bucketName = objectKey.bucketName
    val key = objectKey.key
    val versionId = objectKey.versionId
    val status = objectKey.status
    log.info("Request to create object, key={}, bucket={}, version_id={}, status={}", key, bucketName, versionId, status)
    val objectId = objectKey.id
    val maybeDoc = findById(objectId, Some(versionId)).headOption
    val doc = maybeDoc.getOrElse(
      createDocument(IdField, objectId.toString)
        .put(BucketNameField, bucketName)
        .put(KeyField, key)
        .put(VersionIndexField, objectKey.index)
        .put(VersionField, objectKey.version.entryName)
        .put(VersionIdField, versionId)
    )

    val updatedDocument = doc
      .put(ETagField, objectKey.eTag.orNull)
      .put(ContentMd5Field, objectKey.contentMd5.orNull)
      .put(ContentLengthField, objectKey.contentLength)
      .put(UploadIdField, objectKey.uploadId.orNull)
      .put(PathField, objectKey.objectPath.orNull)
      .put(StatusField, status.entryName)

    Try(collection.update(updatedDocument, true)) match {
      case Failure(ex) =>
        log.error(s"Error creating/updating document, key=$key, bucket=$bucketName", ex)
        throw DatabaseAccessException(s"Error creating/updating `$key` in the bucket `$bucketName`")
      case Success(writeResult) =>
        val docId = writeResult.iterator().asScala.toList.headOption
        if (docId.isEmpty) throw DatabaseAccessException(s"unable to get document id for $bucketName/$key")
        else {
          log.info("Object created/updated, key={}, bucket={}, version_id={}, doc_id={}", key, bucketName, versionId,
            docId.get.getIdValue)
          objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
        }
    }
  }

  private[repositories] def deleteAll(bucketName: String): Int =
    collection.remove(feq(BucketNameField, bucketName)).getAffectedCount

  private[repositories] def findAll(objectId: UUID): List[ObjectKey] = findAllById(objectId).map(ObjectKey(_))

  private[repositories] def findAll(bucketName: String): List[ObjectKey] =
    collection.find(feq(BucketNameField, bucketName)).toScalaList.map(ObjectKey(_))

  private[repositories] def hasObjects(bucketName: String): Boolean =
    findAll(bucketName).exists(_.status == ObjectStatus.Active)

  private def findAllById(objectId: UUID): List[Document] =
    collection.find(feq(IdField, objectId.toString), FindOptions.sort(VersionIndexField, SortOrder.Ascending)).toScalaList

  private def findById(objectId: UUID, maybeVersionId: Option[String]): List[Document] = {
    val versionId = maybeVersionId.getOrElse(NonVersionId(objectId))
    val filter = and(feq(IdField, objectId.toString), feq(VersionIdField, versionId))
    collection.find(filter, FindOptions.sort(VersionIndexField, SortOrder.Ascending)).toScalaList
  }

}

object ObjectCollection {
  def apply(db: Nitrite)(implicit dateTimeProvider: DateTimeProvider): ObjectCollection = new ObjectCollection(db)
}
