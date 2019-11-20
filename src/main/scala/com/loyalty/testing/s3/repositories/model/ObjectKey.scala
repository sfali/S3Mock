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
                     lastModifiedTime: OffsetDateTime)

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
            lastModifiedTime: OffsetDateTime): ObjectKey =
    new ObjectKey(id, bucketName, key, index, version, versionId, eTag, contentMd5, contentLength, lastModifiedTime)

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
      lastModifiedTime = doc.getLastModifiedTime.toOffsetDateTime
    )
}
