package com.loyalty.testing.s3.repositories.model

import akka.http.scaladsl.model.headers.ByteRange
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
                      contentLength: Long,
                      contentRange: ByteRange.Slice) {
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
      eTag = Some(eTag),
      contentMd5 = Some(contentMd5),
      contentLength = contentLength,
      fullContentLength = contentLength,
      objectPath = Some(toObjectDir(bucketName, key, version, versionId))
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
            contentLength: Long = 0,
            contentRange: ByteRange.Slice = ByteRange(0, 0)): UploadInfo =
    new UploadInfo(bucketName, key, version, versionIndex, uploadId, uploadPath, partNumber, eTag, contentMd5,
      contentLength, contentRange)

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
      contentLength = document.getLong(ContentLengthField),
      contentRange = ByteRange(document.getLong(RangeStartField), document.getLong(RangeEndField))
    )
}