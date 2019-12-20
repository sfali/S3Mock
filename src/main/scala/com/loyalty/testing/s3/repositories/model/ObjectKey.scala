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
                     eTag: String,
                     contentMd5: String,
                     contentLength: Long,
                     objectPath: String,
                     lastModifiedTime: OffsetDateTime,
                     deleteMarker: Option[Boolean],
                     uploadId: Option[String],
                     prefixes: List[String]) {
  def actualVersionId: Option[String] =
    version match {
      case BucketVersioning.Enabled => Some(versionId)
      case _ => None
    }
}

object ObjectKey {
  def apply(id: UUID,
            bucketName: String,
            key: String,
            index: Int,
            version: BucketVersioning,
            versionId: String,
            eTag: String,
            contentMd5: String,
            contentLength: Long,
            objectPath: String,
            lastModifiedTime: OffsetDateTime = OffsetDateTime.now,
            deleteMarker: Option[Boolean] = None,
            uploadId: Option[String] = None,
            prefixes: List[String] = Nil): ObjectKey =
    new ObjectKey(
      id,
      bucketName,
      key,
      index,
      version,
      versionId,
      eTag,
      contentMd5,
      contentLength,
      objectPath,
      lastModifiedTime,
      deleteMarker,
      uploadId,
      prefixes
    )

  def apply(doc: Document): ObjectKey =
    ObjectKey(
      id = doc.getUUID(IdField),
      bucketName = doc.getString(BucketNameField),
      key = doc.getString(KeyField),
      index = doc.getInt(VersionIndexField),
      version = BucketVersioning.withName(doc.getString(VersionField)),
      versionId = doc.getString(VersionIdField),
      eTag = doc.getString(ETagField),
      contentMd5 = doc.getString(ContentMd5Field),
      contentLength = doc.getLong(ContentLengthField),
      objectPath = doc.getString(PathField),
      lastModifiedTime = doc.getLastModifiedTime.toOffsetDateTime,
      deleteMarker = doc.getOptionalBoolean(DeleteMarkerField),
      uploadId = doc.getOptionalString(UploadIdField),
      prefixes = doc.getListField(PrefixField)
    )
}
