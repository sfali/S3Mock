package com.loyalty.testing.s3

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time.{decodeZonedDateTimeWithFormatter, encodeZonedDateTimeWithFormatter}

package object notification {

  import parser._

  implicit final val decodeZonedDateTimeDefault: Decoder[ZonedDateTime] =
    decodeZonedDateTimeWithFormatter(ISO_OFFSET_DATE_TIME)

  implicit final val encodeZonedDateTimeDefault: Encoder[ZonedDateTime] =
    encodeZonedDateTimeWithFormatter(ISO_OFFSET_DATE_TIME)

  case class SqsEvent(Records: List[SqsRecord])

  object SqsEvent {
    def apply(sqsRecord: SqsRecord): SqsEvent = SqsEvent(sqsRecord :: Nil)
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

  case class S3Object(key: String,
                      size: Long,
                      eTag: String,
                      versionId: Option[String] = None,
                      sequencer: String = "0055AED6DCD90281E5")

  case class OwnerIdentity(principalId: String = "A3NL1KOZZKExample")

  case class Bucket(name: String,
                    ownerIdentity: OwnerIdentity = OwnerIdentity()) {
    val arn: String = s"arn:aws:s3:::$name"
  }

  case class S3(s3SchemaVersion: String = "1.0",
                configurationId: String,
                bucket: Bucket,
                `object`: S3Object)

  case class ResponseElements(`x-amz-request-id`: String = "C3D13FE58DE4C810",
                              `x-amz-id-2`: String =
                              "FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD")

  case class RequestParameters(sourceIPAddress: String = "0.0.0.0")

  case class UserIdentity(principalId: String = "AIDAJDPLRKLG7UEXAMPLE")

  def generateSqsMessage(notificationMeta: NotificationMeta,
                         notificationData: NotificationData): String = {
    val s3Object = S3Object(notificationData.key,
      notificationData.size,
      notificationData.eTag,
      notificationData.maybeVersionId)
    val bucket = Bucket(notificationData.bucketName)
    val s3 = S3(configurationId = notificationMeta.configName, bucket = bucket, `object` = s3Object)
    val eventName = s"${notificationMeta.notificationType}:${notificationData.operation}"
    toJsonString(SqsEvent(SqsRecord(eventName = eventName, s3 = s3)))
  }

}
