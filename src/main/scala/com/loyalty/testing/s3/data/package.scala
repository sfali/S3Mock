package com.loyalty.testing.s3

import com.loyalty.testing.s3.notification.Notification

package object data {

  case class InitialBucket(bucketName: String,
                           region: Option[String] = None,
                           enableVersioning: Boolean = false)

  case class BootstrapConfiguration(initialBuckets: List[InitialBucket] = Nil,
                                    notifications: List[Notification] = Nil)

}
