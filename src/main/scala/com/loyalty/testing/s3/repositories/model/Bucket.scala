package com.loyalty.testing.s3.repositories.model

import java.util.UUID

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration}
import org.dizitart.no2.Document

case class Bucket(id: UUID,
                  bucketName: String,
                  region: String,
                  version: BucketVersioning)

object Bucket {
  def apply(id: UUID,
            bucketName: String,
            region: String,
            version: BucketVersioning): Bucket =
    new Bucket(id, bucketName, region, version)

  def apply(bucketName: String,
            region: String,
            version: BucketVersioning): Bucket =
    Bucket(bucketName.toUUID, bucketName, region, version)

  def apply(bucketName: String,
            bucketConfiguration: CreateBucketConfiguration,
            version: BucketVersioning): Bucket =
    Bucket(bucketName, bucketConfiguration.locationConstraint, version)

  def apply(document: Document): Bucket =
    Bucket(
      id = document.getUUID(IdField),
      bucketName = document.getString(BucketNameField),
      region = document.getString(RegionField),
      version = BucketVersioning.withName(document.getString(VersionField))
    )
}
