package com.loyalty.testing.s3.repositories.model

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.request.BucketVersioning
import org.dizitart.no2.Document

case class UploadInfo(bucketName: String,
                      key: String,
                      version: BucketVersioning,
                      versionIndex: Int,
                      uploadId: String,
                      uploadPath: String,
                      partNumber: Int,
                      eTag: String,
                      contentMd5: String,
                      contentLength: Long) {
  def toObjectKey: ObjectKey = {
    val objectId = createObjectId(bucketName, key)
    val versionId = createVersionId(objectId, versionIndex)
    ObjectKey(
      id = objectId,
      bucketName = bucketName,
      key = key,
      index = versionIndex,
      version = version,
      versionId = versionId,
      eTag = eTag,
      contentMd5 = contentMd5,
      contentLength = contentLength,
      objectPath = toObjectDir(bucketName, key, version, versionId)
    )
  }
}

object UploadInfo {
  def apply(bucketName: String,
            key: String,
            version: BucketVersioning,
            versionIndex: Int,
            uploadId: String,
            uploadPath: String,
            partNumber: Int = 0,
            eTag: String = "",
            contentMd5: String = "",
            contentLength: Long = 0): UploadInfo =
    new UploadInfo(bucketName, key, version, versionIndex, uploadId, uploadPath, partNumber, eTag, contentMd5, contentLength)

  def apply(document: Document): UploadInfo =
    UploadInfo(
      bucketName = document.getString(BucketNameField),
      key = document.getString(KeyField),
      version = BucketVersioning.withName(document.getString(VersionField)),
      versionIndex = document.getInt(VersionIndexField),
      uploadId = document.getString(UploadIdField),
      uploadPath = document.getString(PathField),
      partNumber = document.getInt(PartNumberField),
      eTag = document.getString(ETagField),
      contentMd5 = document.getString(ContentMd5Field),
      contentLength = document.getLong(ContentLengthField)
    )
}