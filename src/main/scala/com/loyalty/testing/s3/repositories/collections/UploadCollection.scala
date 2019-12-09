package com.loyalty.testing.s3.repositories.collections

import akka.Done
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.UploadInfo
import org.dizitart.no2.IndexOptions.indexOptions
import org.dizitart.no2.IndexType._
import org.dizitart.no2._
import org.dizitart.no2.filters.Filters.{eq => feq, _}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class UploadCollection(db: Nitrite) {

  import Document._

  private val log = LoggerFactory.getLogger(classOf[UploadCollection])

  private[repositories] val collection = db.getCollection("upload_staging")
  if (!collection.hasIndex(PartNumberField)) {
    collection.createIndex(PartNumberField, indexOptions(NonUnique))
  }

  private[repositories] def createUpload(uploadInfo: UploadInfo): Done = {
    val uploadId = uploadInfo.uploadId
    val bucketName = uploadInfo.bucketName
    val key = uploadInfo.key
    val partNumber = uploadInfo.partNumber
    log.info("upload part, upload_id={}, part_number={},bucket_name={}, key={}", uploadId, partNumber, bucketName, key)
    val document =
      findById(uploadId, partNumber) match {
        case Nil =>
          createDocument(UploadIdField, uploadId)
            .put(BucketNameField, bucketName)
            .put(KeyField, key)
            .put(VersionField, uploadInfo.version.entryName)
            .put(VersionIndexField, uploadInfo.versionIndex)
            .put(PartNumberField, partNumber)
            .put(ETagField, uploadInfo.eTag)
            .put(ContentMd5Field, uploadInfo.contentMd5)
            .put(ContentLengthField, uploadInfo.contentLength)
        case document :: Nil =>
          val other = UploadInfo(document)
          if (other != uploadInfo) {
            log.warn(
              """Attempt to access upload of different object, upload_id={}, bucket_name={}, key={},
                |other_bucket_name={}, other_key={}""".stripMargin, uploadId,
              bucketName, key, other.bucketName, other.key)
            throw new IllegalArgumentException("Attempt to access upload of different object")
          }
          document
        case _ => throw new IllegalStateException(s"Multiple documents found for $uploadId/$partNumber")
      }
    Try(collection.update(document, true)) match {
      case Failure(ex) =>
        log.error(s"Error creating/updating document, key=$key, bucket=$bucketName, upload_id=$uploadId", ex)
        throw DatabaseAccessException(s"Error creating/updating `$key` in the bucket `$bucketName`")
      case Success(writeResult) =>
        val docId = writeResult.iterator().asScala.toList.headOption
        if (docId.isEmpty) throw DatabaseAccessException(s"unable to get document id for $bucketName/$key/$uploadId")
        else {
          log.info("Initiated multi part upload, upload_id={}, doc_id={}", uploadId, docId.get.getIdValue)
          Done
        }

    }
  }

  private[repositories] def insert(elements: Document*): Int =
    collection.insert(elements.asJava.toArray(Array.ofDim[Document](elements.size))).getAffectedCount

  private[repositories] def deleteUpload(uploadId: String, partNumber: Int): Int =
    findById(uploadId, partNumber) match {
      case Nil => throw new RuntimeException(s"upload not found: $uploadId/$partNumber")
      case document :: Nil => collection.remove(document).getAffectedCount
      case _ => throw new IllegalStateException(s"Multiple documents found for $uploadId/$partNumber")

    }

  private[repositories] def deleteAll(uploadId: String): Int =
    collection.remove(feq(UploadIdField, uploadId)).getAffectedCount

  private[repositories] def getUpload(uploadId: String, partNumber: Int): Option[UploadInfo] =
    findById(uploadId, partNumber) match {
      case Nil => None
      case document :: Nil => Some(UploadInfo(document))
      case _ => throw new IllegalStateException(s"Multiple documents found for $uploadId")
    }

  private def findById(uploadId: String, partNumber: Int) =
    collection.find(and(feq(UploadIdField, uploadId), feq(PartNumberField, partNumber))).toScalaList
}

object UploadCollection {
  def apply(db: Nitrite): UploadCollection = new UploadCollection(db)
}
