package com.loyalty.testing.s3

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

import io.circe._
import io.circe.generic.semiauto._
import io.circe.java8.time.{decodeZonedDateTime, encodeZonedDateTime}

package object notification {

  import parser._

  implicit final val decodeZonedDateTimeDefault: Decoder[ZonedDateTime] =
    decodeZonedDateTime(ISO_OFFSET_DATE_TIME)

  implicit final val encodeZonedDateTimeDefault: Encoder[ZonedDateTime] =
    encodeZonedDateTime(ISO_OFFSET_DATE_TIME)

  case class SqsEvent(Records: List[SqsRecord])

  object SqsEvent {
    def apply(sqsRecord: SqsRecord): SqsEvent = SqsEvent(sqsRecord :: Nil)

    implicit val SqsEventDecoder: Decoder[SqsEvent] = deriveDecoder[SqsEvent]
    implicit val SqsEventEncoder: Encoder[SqsEvent] = deriveEncoder[SqsEvent]
  }

  case class SqsRecord(eventVersion: String = "2.0",
                       eventSource: String = "aws:s3",
                       awsRegion: String = "us-east-1",
                       eventTime: ZonedDateTime = ZonedDateTime.now(),
                       eventName: String = "ObjectCreated:Put",
                       userIdentity: UserIdentity = UserIdentity(),
                       requestParameters: RequestParameters =
                       RequestParameters(),
                       responseElements: ResponseElements = ResponseElements(),
                       s3: S3)

  object SqsRecord {
    implicit val SqsRecordDecoder: Decoder[SqsRecord] = deriveDecoder[SqsRecord]
    implicit val SqsRecordEncoder: Encoder[SqsRecord] = deriveEncoder[SqsRecord]
  }

  case class S3Object(key: String,
                      size: Long,
                      eTag: String,
                      versionId: Option[String] = None,
                      sequencer: String = "0055AED6DCD90281E5")

  object S3Object {
    implicit val S3ObjectDecoder: Decoder[S3Object] = deriveDecoder[S3Object]
    implicit val S3ObjectEncoder: Encoder[S3Object] = deriveEncoder[S3Object]
  }

  case class OwnerIdentity(principalId: String = "A3NL1KOZZKExample")

  object OwnerIdentity {
    implicit val OwnerIdentityDecoder: Decoder[OwnerIdentity] =
      deriveDecoder[OwnerIdentity]
    implicit val OwnerIdentityEncoder: Encoder[OwnerIdentity] =
      deriveEncoder[OwnerIdentity]
  }

  case class Bucket(name: String,
                    ownerIdentity: OwnerIdentity = OwnerIdentity()) {
    val arn: String = s"arn:aws:s3:::$name"
  }

  object Bucket {
    implicit val BucketDecoder: Decoder[Bucket] = deriveDecoder[Bucket]
    implicit val BucketEncoder: Encoder[Bucket] = deriveEncoder[Bucket]
  }

  case class S3(s3SchemaVersion: String = "1.0",
                configurationId: String,
                bucket: Bucket,
                `object`: S3Object)

  object S3 {
    implicit val S3Decoder: Decoder[S3] = deriveDecoder[S3]
    implicit val S3Encoder: Encoder[S3] = deriveEncoder[S3]
  }

  case class ResponseElements(`x-amz-request-id`: String = "C3D13FE58DE4C810",
                              `x-amz-id-2`: String =
                              "FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD")

  object ResponseElements {
    implicit val ResponseElementsDecoder: Decoder[ResponseElements] =
      deriveDecoder[ResponseElements]
    implicit val ResponseElementsEncoder: Encoder[ResponseElements] =
      deriveEncoder[ResponseElements]
  }

  case class RequestParameters(sourceIPAddress: String = "0.0.0.0")

  object RequestParameters {
    implicit val RequestParametersDecoder: Decoder[RequestParameters] =
      deriveDecoder[RequestParameters]
    implicit val RequestParametersEncoder: Encoder[RequestParameters] =
      deriveEncoder[RequestParameters]
  }

  case class UserIdentity(principalId: String = "AIDAJDPLRKLG7UEXAMPLE")

  object UserIdentity {
    implicit val UserIdentityDecoder: Decoder[UserIdentity] =
      deriveDecoder[UserIdentity]
    implicit val UserIdentityEncoder: Encoder[UserIdentity] =
      deriveEncoder[UserIdentity]
  }

  def generateSqsMessage(name: String,
                         notificationData: NotificationData): String = {
    val s3Object = S3Object(notificationData.key,
      notificationData.size,
      notificationData.eTag,
      notificationData.maybeVersionId)
    val bucket = Bucket(notificationData.bucketName)
    val s3 = S3(configurationId = name, bucket = bucket, `object` = s3Object)
    toJsonString(SqsEvent(SqsRecord(s3 = s3)))
  }

}
