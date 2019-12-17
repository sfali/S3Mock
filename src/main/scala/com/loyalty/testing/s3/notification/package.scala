package com.loyalty.testing.s3

import java.time.ZonedDateTime

import com.loyalty.testing.s3.parser._
import io.circe.Encoder._
import io.circe.generic.auto._

package object notification {

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

  object S3Object {
    def apply(key: String,
              size: Long,
              eTag: String,
              versionId: Option[String],
              sequencer: String = "0055AED6DCD90281E5"): S3Object =
      new S3Object(key, size, eTag, versionId, sequencer)

    def apply(notificationData: NotificationData): S3Object =
      S3Object(
        key = notificationData.key,
        size = notificationData.size,
        eTag = notificationData.eTag,
        versionId = notificationData.maybeVersionId
      )
  }

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

  @deprecated
  def generateSqsMessage(notificationMeta: NotificationMeta,
                         notificationData: NotificationData): String = {
    val bucket = Bucket(notificationData.bucketName)
    val s3 = S3(configurationId = notificationMeta.configName, bucket =  Bucket(notificationData.bucketName),
      `object` = S3Object(notificationData))
    val eventName = s"${notificationMeta.notificationType}:${notificationData.operation}"
    toJsonString(SqsEvent(SqsRecord(eventName = eventName, s3 = s3)))
  }

  def generateMessage(notificationData: NotificationData,
                      notification: Notification): String = {
    val s3 = S3(configurationId = notification.name, bucket = Bucket(notificationData.bucketName),
      `object` = S3Object(notificationData))
    val eventName = s"${notification.notificationType}:${notificationData.operation}"
    toJsonString(SqsEvent(SqsRecord(eventName = eventName, s3 = s3)))
  }

}
