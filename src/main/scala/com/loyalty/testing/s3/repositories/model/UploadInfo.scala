package com.loyalty.testing.s3.repositories.model

import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.request.BucketVersioning
import org.dizitart.no2.Document

case class UploadInfo(bucketName: String,
                      key: String,
                      version: BucketVersioning,
                      versionIndex: Int,
                      uploadId: String,
                      partNumber: Int)

object UploadInfo {
  def apply(bucketName: String,
            key: String,
            version: BucketVersioning,
            versionIndex: Int,
            uploadId: String,
            partNumber: Int): UploadInfo =
    new UploadInfo(bucketName, key, version, versionIndex, uploadId, partNumber)

  def apply(document: Document): UploadInfo =
    UploadInfo(
      bucketName = document.getString(BucketNameField),
      key = document.getString(KeyField),
      version = BucketVersioning.withName(document.getString(VersionField)),
      versionIndex = document.getInt(VersionIndexField),
      uploadId = document.getString(UploadIdField),
      partNumber = document.getInt(PartNumberField)
    )
}