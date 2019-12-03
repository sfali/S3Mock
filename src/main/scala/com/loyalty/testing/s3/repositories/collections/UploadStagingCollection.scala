package com.loyalty.testing.s3.repositories.collections

import akka.Done
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.UploadInfo
import org.dizitart.no2.IndexOptions.indexOptions
import org.dizitart.no2.IndexType.NonUnique
import org.dizitart.no2._
import org.dizitart.no2.filters.Filters.{eq => feq, _}
import org.slf4j.LoggerFactory

class UploadStagingCollection(db: Nitrite) {

  import Document._

  private val log = LoggerFactory.getLogger(classOf[UploadStagingCollection])

  private[repositories] val collection = db.getCollection("upload_staging")
  if (!collection.hasIndex(PartNumberField)) {
    collection.createIndex(PartNumberField, indexOptions(NonUnique))
  }

  private[repositories] def createUpload(uploadInfo: UploadInfo): Done = {
    val uploadId = uploadInfo.uploadId
    val bucketName = uploadInfo.bucketName
    val key = uploadInfo.key
    log.info("Initiating multi part upload, upload_id={}, bucket_name={}, key={}", uploadId, bucketName, key)
    val document =
      findById(uploadId, uploadInfo.partNumber) match {
        case Nil =>
          createDocument(UploadIdField, uploadId)
            .put(BucketNameField, bucketName)
            .put(KeyField, key)
            .put(VersionField, uploadInfo.version.entryName)
            .put(VersionIndexField, uploadInfo.versionIndex)
            .put(PartNumberField, uploadInfo.partNumber)
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
          }
          document
        case _ => throw new IllegalStateException(s"Multiple documents found for $uploadId")
      }
    log.info("Initiated multi part upload, upload_id={}, doc_id={}", uploadId, document.getId.getIdValue)
    Done
  }

  private[repositories] def getUpload(uploadId: String, partNumber: Int): Option[UploadInfo] =
    findById(uploadId, partNumber) match {
      case Nil => None
      case document :: Nil => Some(UploadInfo(document))
      case _ => throw new IllegalStateException(s"Multiple documents found for $uploadId")
    }

  private def findById(uploadId: String, partNumber: Int) =
    collection.find(and(feq(UploadIdField, uploadId), feq(PartNumberField, partNumber))).toScalaList
}

object UploadStagingCollection {
  def apply(db: Nitrite): UploadStagingCollection = new UploadStagingCollection(db)
}
