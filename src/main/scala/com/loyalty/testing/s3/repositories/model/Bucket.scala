package com.loyalty.testing.s3.repositories.model

import java.nio.file.Path

import com.loyalty.testing.s3.repositories._
import org.dizitart.no2.Document

case class Bucket(bucketName: String,
                  region: String,
                  bucketPath: Path,
                  version: Option[String])

object Bucket {
  def apply(document: Document): Bucket =
    new Bucket(
      bucketName = document.getString(BucketNameField),
      region = document.getString(RegionField),
      bucketPath = document.getPath(BucketPathField),
      version = document.getOptionalString(VersionField)
    )
}
