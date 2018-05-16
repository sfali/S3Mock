package com.loyalty.testing.s3.notification

import io.circe.Decoder
import io.circe.Decoder.enumDecoder
import io.circe.Encoder
import io.circe.Encoder.enumEncoder

object NotificationType extends Enumeration {

  type NotificationType = Value

  val ObjectCreateAll: NotificationType = Value

  implicit val encoder: Encoder[NotificationType] = enumEncoder(NotificationType)
  implicit val decoder: Decoder[NotificationType] = enumDecoder(NotificationType)
}
