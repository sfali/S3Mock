package com.loyalty.testing.s3.repositories.model

import java.util.UUID

import com.loyalty.testing.s3._
import com.loyalty.testing.s3.data.InitialBucket
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

  def apply(bucketName: String): Bucket =
    Bucket(bucketName.toUUID, bucketName, defaultRegion, BucketVersioning.NotExists)

  def apply(bucketName: String,
            region: String,
            version: BucketVersioning): Bucket =
    Bucket(bucketName.toUUID, bucketName, region, version)

  def apply(bucketName: String,
            bucketConfiguration: CreateBucketConfiguration,
            version: BucketVersioning): Bucket =
    Bucket(bucketName, bucketConfiguration.locationConstraint, version)

  def apply(initialBucket: InitialBucket): Bucket =
    Bucket(
      bucketName = initialBucket.bucketName,
      region = initialBucket.region.getOrElse(defaultRegion),
      version = if (initialBucket.enableVersioning) BucketVersioning.Enabled else BucketVersioning.NotExists
    )

  def apply(document: Document): Bucket =
    Bucket(
      id = document.getUUID(IdField),
      bucketName = document.getString(BucketNameField),
      region = document.getString(RegionField),
      version = BucketVersioning.withName(document.getString(VersionField))
    )
}
