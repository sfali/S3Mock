package com.loyalty.testing.s3.actor.model.`object`

import java.util.UUID

import akka.actor.typed.ActorRef
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3.actor.model.Event
import com.loyalty.testing.s3.createObjectId
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, UploadInfo}
import com.loyalty.testing.s3.request.PartInfo

sealed trait Command

sealed trait CommandWithReply extends Command {
  val replyTo: ActorRef[Event]
}

sealed trait ObjectInput extends CommandWithReply {
  val bucket: Bucket
  val key: String
  lazy val objectId: UUID = createObjectId(bucket.bucketName, key)
}

sealed trait UploadInput extends ObjectInput {
  val uploadId: String
}

private[actor] case object Shutdown extends Command

private[actor] case object DatabaseError extends Command

private[actor] final case class ReplyToSender(reply: Event,
                                       replyTo: ActorRef[Event],
                                       maybeObjectKey: Option[ObjectKey] = None) extends Command

private[actor] case object InitializeSnapshot extends Command

private[actor] final case class ObjectResult(values: List[ObjectKey]) extends Command

final case class PutObject(bucket: Bucket,
                           key: String,
                           contentSource: Source[ByteString, _],
                           replyTo: ActorRef[Event]) extends ObjectInput

final case class GetObjectMeta(replyTo: ActorRef[Event]) extends CommandWithReply

final case class GetObject(bucket: Bucket,
                           key: String,
                           maybeVersionId: Option[String],
                           maybeRange: Option[ByteRange],
                           replyTo: ActorRef[Event]) extends ObjectInput

final case class DeleteObject(bucket: Bucket,
                              key: String,
                              maybeVersionId: Option[String],
                              replyTo: ActorRef[Event]) extends ObjectInput

final case class InitiateMultiPartUpload(bucket: Bucket,
                                         key: String,
                                         replyTo: ActorRef[Event]) extends ObjectInput

private[actor] final case class UploadInfoCreated(uploadInfo: UploadInfo,
                                           replyTo: ActorRef[Event]) extends CommandWithReply

final case class UploadPart(bucket: Bucket,
                            key: String,
                            uploadId: String,
                            partNumber: Int,
                            contentSource: Source[ByteString, _],
                            replyTo: ActorRef[Event]) extends UploadInput

private[actor] final case class PartSaved(uploadInfo: UploadInfo,
                                   replyTo: ActorRef[Event]) extends CommandWithReply

final case class CompleteUpload(bucket: Bucket,
                                key: String,
                                uploadId: String,
                                parts: List[PartInfo],
                                replyTo: ActorRef[Event]) extends UploadInput

private[actor] final case class ResetUploadInfo(objectKey: ObjectKey,
                                         replyTo: ActorRef[Event]) extends CommandWithReply
