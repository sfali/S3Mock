package com.loyalty.testing.s3.notification

import io.circe.Decoder.enumDecoder
import io.circe.Encoder.enumEncoder
import io.circe.{Decoder, Encoder}

object DestinationType extends Enumeration {

  type DestinationType = Value

  val Sqs, Sns = Value

  implicit val encoder: Encoder[DestinationType] = enumEncoder(DestinationType)
  implicit val decoder: Decoder[DestinationType] = enumDecoder(DestinationType)
}
