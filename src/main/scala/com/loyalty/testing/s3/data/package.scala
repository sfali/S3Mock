package com.loyalty.testing.s3

import akka.http.scaladsl.model.headers.ByteRange
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.{ObjectKey, ObjectStatus}

package object data {

  case class InitialBucket(bucketName: String,
                           region: Option[String] = None,
                           enableVersioning: Boolean = false)

  case class BootstrapConfiguration(initialBuckets: List[InitialBucket] = Nil,
                                    notifications: List[Notification] = Nil)

  case class ObjectInfo(bucketName: String,
                        key: String,
                        eTag: Option[String],
                        contentLength: Long,
                        status: ObjectStatus,
                        partsCount: Option[Int],
                        versionId: Option[String] = None,
                        contentRange: Option[ByteRange.Slice] = None)

  object ObjectInfo {
    def apply(bucketName: String,
              key: String,
              eTag: Option[String] = None,
              contentLength: Long = 0L,
              status: ObjectStatus = ObjectStatus.Active,
              partsCount: Option[Int] = None,
              versionId: Option[String] = None,
              contentRange: Option[ByteRange.Slice] = None): ObjectInfo =
      new ObjectInfo(bucketName, key, eTag, contentLength, status, partsCount, versionId, contentRange)

    def apply(objectKey: ObjectKey): ObjectInfo =
      ObjectInfo(objectKey.bucketName, objectKey.key, objectKey.eTag, objectKey.contentLength, objectKey.status,
        objectKey.partsCount, objectKey.actualVersionId, objectKey.contentRange)
  }

}
