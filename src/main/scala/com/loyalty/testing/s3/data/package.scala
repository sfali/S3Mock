package com.loyalty.testing.s3

import com.loyalty.testing.s3.notification.Notification
import io.circe.generic.semiauto._
import io.circe._

package object data {

  case class InitialBucket(bucketName: String,
                           enableVersioning: Boolean = false)

  object InitialBucket {
    implicit val InitialBucketDecoder: Decoder[InitialBucket] =
      deriveDecoder[InitialBucket]
    implicit val InitialBucketEncoder: Encoder[InitialBucket] =
      deriveEncoder[InitialBucket]
  }

  case class BootstrapConfiguration(initialBuckets: List[InitialBucket] = Nil,
                                    Notifications: List[Notification] = Nil)

  object BootstrapConfiguration {
    implicit val BootstrapDataDecoder: Decoder[BootstrapConfiguration] =
      deriveDecoder[BootstrapConfiguration]
    implicit val BootstrapDataEncoder: Encoder[BootstrapConfiguration] =
      deriveEncoder[BootstrapConfiguration]
  }
}
