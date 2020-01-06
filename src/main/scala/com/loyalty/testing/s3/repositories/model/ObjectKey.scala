package com.loyalty.testing.s3.repositories.model

import java.time.OffsetDateTime
import java.util.UUID

import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.request.BucketVersioning
import org.dizitart.no2.Document

case class ObjectKey(id: UUID,
                     bucketName: String,
                     key: String,
                     index: Int,
                     version: BucketVersioning,
                     versionId: String,
                     status: ObjectStatus,
                     eTag: Option[String],
                     contentMd5: Option[String],
                     contentLength: Long,
                     objectPath: Option[String],
                     lastModifiedTime: OffsetDateTime,
                     uploadId: Option[String],
                     deleteMarker: Option[Boolean] = None) {
  def actualVersionId: Option[String] =
    version match {
      case BucketVersioning.Enabled => Some(versionId)
      case _ => None
    }

  def isDeleted: Boolean = status == ObjectStatus.Deleted || status == ObjectStatus.DeleteMarkerDeleted
}

object ObjectKey {
  def apply(id: UUID,
            bucketName: String,
            key: String,
            index: Int,
            version: BucketVersioning,
            versionId: String,
            status: ObjectStatus = ObjectStatus.Active,
            eTag: Option[String] = None,
            contentMd5: Option[String] = None,
            contentLength: Long = 0L,
            objectPath: Option[String] = None,
            lastModifiedTime: OffsetDateTime = OffsetDateTime.now,
            uploadId: Option[String] = None,
            deleteMarker: Option[Boolean] = None): ObjectKey =
    new ObjectKey(
      id,
      bucketName,
      key,
      index,
      version,
      versionId,
      status,
      eTag,
      contentMd5,
      contentLength,
      objectPath,
      lastModifiedTime,
      uploadId,
      deleteMarker
    )

  def apply(doc: Document): ObjectKey =
    ObjectKey(
      id = doc.getUUID(IdField),
      bucketName = doc.getString(BucketNameField),
      key = doc.getString(KeyField),
      index = doc.getInt(VersionIndexField),
      version = BucketVersioning.withName(doc.getString(VersionField)),
      versionId = doc.getString(VersionIdField),
      status = ObjectStatus.withName(doc.getString(StatusField)),
      eTag = doc.getOptionalString(ETagField),
      contentMd5 = doc.getOptionalString(ContentMd5Field),
      contentLength = doc.getLong(ContentLengthField),
      objectPath = doc.getOptionalString(PathField),
      lastModifiedTime = doc.getLastModifiedTime.toOffsetDateTime,
      uploadId = doc.getOptionalString(UploadIdField)
    )
}
