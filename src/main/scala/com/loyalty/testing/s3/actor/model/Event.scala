package com.loyalty.testing.s3.actor.model

import java.util.UUID

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, UploadInfo}
import com.loyalty.testing.s3.response.{BucketContent, DeleteResult}

sealed trait Event

case class NoSuchBucketExists(bucketId: UUID) extends Event

case class NoSuchKeyExists(bucketName: String, key: String) extends Event

case class BucketNotEmpty(bucketName: String) extends Event

case object InvalidAccess extends Event

case object NotificationsCreated extends Event

final case class BucketInfo(bucket: Bucket) extends Event

final case class BucketDeleted(bucketName: String) extends Event

final case class NotificationsInfo(notifications: List[Notification]) extends Event

final case class ObjectInfo(objectKey: ObjectKey) extends Event

final case class ObjectContent(objectKey: ObjectKey, content: Source[ByteString, _]) extends Event

final case class CopyObjectInfo(objectKey: ObjectKey, sourceVersionId: Option[String]) extends Event

final case class CopyPartInfo(uploadInfo: UploadInfo, sourceVersionId: Option[String]) extends Event

final case class MultiPartUploadedInitiated(uploadId: String) extends Event

final case class PartUploaded(uploadInfo: UploadInfo) extends Event

final case class ListBucketContent(contents: List[BucketContent]) extends Event

final case class DeleteObjectsResult(result: DeleteResult) extends Event

case object NoSuchUpload extends Event

case object InvalidPartOrder extends Event

final case class InvalidPart(partNumber: Int) extends Event

final case class BucketAlreadyExists(bucket: Bucket) extends Event

final case class NoSuchVersionExists(bucketName: String, key: String, versionId: String) extends Event

case object InternalError extends Event
