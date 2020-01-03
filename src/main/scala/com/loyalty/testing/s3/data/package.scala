package com.loyalty.testing.s3

import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.ObjectKey

package object data {

  case class InitialBucket(bucketName: String,
                           region: Option[String] = None,
                           enableVersioning: Boolean = false)

  case class BootstrapConfiguration(initialBuckets: List[InitialBucket] = Nil,
                                    notifications: List[Notification] = Nil)

  case class ObjectInfo(bucketName: String,
                        key: String,
                        eTag: String,
                        contentLength: Long,
                        versionId: Option[String] = None)

  object ObjectInfo {
    def apply(bucketName: String,
              key: String,
              eTag: String,
              contentLength: Long,
              versionId: Option[String] = None): ObjectInfo =
      new ObjectInfo(bucketName, key, eTag, contentLength, versionId)

    def apply(objectKey: ObjectKey): ObjectInfo =
      ObjectInfo(objectKey.bucketName, objectKey.key, objectKey.eTag.getOrElse(""), objectKey.contentLength, objectKey.actualVersionId)
  }

}
