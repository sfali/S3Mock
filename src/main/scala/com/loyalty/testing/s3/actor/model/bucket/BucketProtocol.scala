package com.loyalty.testing.s3.actor.model.bucket

import akka.actor.typed.ActorRef
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3.actor.Event
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.{PartInfo, VersioningConfiguration}

sealed trait BucketProtocol

sealed trait BucketProtocolWithReply extends BucketProtocol {
  val replyTo: ActorRef[Event]
}

private[actor] case object Shutdown extends BucketProtocol

private[actor] case object InitializeSnapshot extends BucketProtocol

private[actor] case object NoSuchBucket extends BucketProtocol

private[actor] case object DatabaseError extends BucketProtocol

private[actor] final case class ReplyToSender(reply: Event, replyTo: ActorRef[Event]) extends BucketProtocol

private[actor] final case class NewBucketCreated(bucket: Bucket,
                                                 replyTo: ActorRef[Event]) extends BucketProtocolWithReply

private[actor] final case class BucketResult(bucket: Bucket) extends BucketProtocol

private[actor] final case class VersioningSet(updatedBucket: Bucket,
                                              replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class CreateBucket(bucket: Bucket, replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class GetBucket(replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class GetBucketNotifications(replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class SetBucketVersioning(versioningConfiguration: VersioningConfiguration,
                                     replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class CreateBucketNotifications(notifications: List[Notification],
                                           replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class PutObjectWrapper(key: String,
                                  contentSource: Source[ByteString, _],
                                  replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class GetObjectMetaWrapper(key: String, replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class GetObjectWrapper(key: String,
                                  maybeVersionId: Option[String] = None,
                                  maybeRange: Option[ByteRange] = None,
                                  replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class DeleteObjectWrapper(key: String,
                                     maybeVersionId: Option[String] = None,
                                     replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class InitiateMultiPartUploadWrapper(key: String,
                                                replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class UploadPartWrapper(key: String,
                                   uploadId: String,
                                   partNumber: Int,
                                   contentSource: Source[ByteString, _],
                                   replyTo: ActorRef[Event]) extends BucketProtocolWithReply

final case class CompleteUploadWrapper(key: String,
                                       uploadId: String,
                                       parts: List[PartInfo],
                                       replyTo: ActorRef[Event]) extends BucketProtocolWithReply