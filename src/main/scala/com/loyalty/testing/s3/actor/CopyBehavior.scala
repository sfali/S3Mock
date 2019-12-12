package com.loyalty.testing.s3.actor

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model.headers.ByteRange
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.Command
import com.loyalty.testing.s3.actor.model.bucket.{GetObjectWrapper, PutObjectWrapper, UploadPartWrapper, Command => BucketCommand}
import com.loyalty.testing.s3.actor.model.{CopyObjectInfo, CopyPartInfo, Event, ObjectContent, ObjectInfo, PartUploaded}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}

import scala.concurrent.duration._

class CopyBehavior(context: ActorContext[Command],
                   objectIO: ObjectIO,
                   database: NitriteDatabase)
  extends AbstractBehavior[Command](context) {

  import CopyBehavior._

  context.setReceiveTimeout(2.minutes, Shutdown)
  private val eventResponseWrapper = context.messageAdapter[Event](EventWrapper.apply)
  private var sourceVersionId: Option[String] = None

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case Copy(sourceBucketName, sourceKey, targetBucketName, targetKey, maybeSourceVersionId, replyTo) =>
        context.self ! GetObject
        copyOperation(sourceBucketName, sourceKey, targetBucketName, targetKey, None, None, maybeSourceVersionId, replyTo)

      case CopyPart(sourceBucketName, sourceKey, targetBucketName, targetKey, uploadId, partNumber, range,
      maybeSourceVersionId, replyTo) =>
        context.self ! GetObject
        copyOperation(sourceBucketName, sourceKey, targetBucketName, targetKey, Some(PartInfo(uploadId, partNumber)),
          range, maybeSourceVersionId, replyTo)

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Invalid command in onMessage: {}", other)
        Behaviors.unhandled
    }

  private def copyOperation(sourceBucketName: String,
                            sourceKey: String,
                            targetBucketName: String,
                            targetKey: String,
                            partInfo: Option[PartInfo],
                            maybeRange: Option[ByteRange],
                            maybeSourceVersionId: Option[String],
                            replyTo: ActorRef[Event]): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case GetObject =>
        context.log.info(
          """Copy Object/Part: source_bucket_name={}, source_key={},  target_bucket_name={},
            | target_key={}""".stripMargin.replaceNewLine, sourceBucketName, sourceKey, targetBucketName, targetKey)
        getBucketActor(sourceBucketName) ! GetObjectWrapper(sourceKey, maybeSourceVersionId, maybeRange, eventResponseWrapper)
        Behaviors.same

      case EventWrapper(ObjectContent(objectKey, content)) if partInfo.isDefined =>
        sourceVersionId = objectKey.actualVersionId
        val uploadId = partInfo.get.uploadId
        val partNumber = partInfo.get.partNumber
        context.log.info(
          """Copy part: Got source content: source_bucket_name={}, source_key={},  target_bucket_name={},
            | target_key={}, source_version_id={}, upload_id={}, part_number={}""".stripMargin.replaceNewLine,
          sourceBucketName, sourceKey, targetBucketName, targetKey, sourceVersionId, uploadId, partNumber)
        getBucketActor(targetBucketName) ! UploadPartWrapper(targetKey, uploadId, partNumber, content, eventResponseWrapper)
        Behaviors.same

      case EventWrapper(ObjectContent(objectKey, content)) =>
        sourceVersionId = objectKey.actualVersionId
        context.log.info(
          """Copy object: Got source content: source_bucket_name={}, source_key={},  target_bucket_name={},
            | target_key={}, source_version_id={}, range={}""".stripMargin.replaceNewLine, sourceBucketName, sourceKey,
          targetBucketName, targetKey, sourceVersionId, maybeRange)
        getBucketActor(targetBucketName) ! PutObjectWrapper(targetKey, content, eventResponseWrapper)
        Behaviors.same

      case EventWrapper(PartUploaded(uploadInfo)) =>
        replyTo ! CopyPartInfo(uploadInfo, sourceVersionId)
        Behaviors.same

      case EventWrapper(ObjectInfo(objectKey)) if objectKey.deleteMarker.contains(true) =>
        replyTo ! ObjectInfo(objectKey)
        Behaviors.same

      case EventWrapper(ObjectInfo(objectKey)) =>
        replyTo ! CopyObjectInfo(objectKey, sourceVersionId)
        Behaviors.same

      case EventWrapper(event) =>
        context.log.warn(
          """Error response: event={}, source_bucket_name={}, source_key={}, target_bucket_name={},
            | target_key={}""".stripMargin.replaceNewLine, event, sourceBucketName, sourceKey, targetBucketName, targetKey)
        replyTo ! event
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Invalid command: {}", other)
        Behaviors.unhandled
    }

  private def getBucketActor(bucketName: String): ActorRef[BucketCommand] = {
    val name = bucketName.toUUID.toString
    context.child(name) match {
      case Some(value) => value.unsafeUpcast[BucketCommand]
      case None => context.spawn(BucketOperationsBehavior(objectIO, database), name)
    }
  }
}

object CopyBehavior {

  def apply(objectIO: ObjectIO,
            database: NitriteDatabase): Behavior[Command] =
    Behaviors.setup[Command](context => new CopyBehavior(context, objectIO, database))

  sealed trait Command

  private final case object Shutdown extends Command

  final case class Copy(sourceBucketName: String,
                        sourceKey: String,
                        targetBucketName: String,
                        targetKey: String,
                        maybeSourceVersionId: Option[String],
                        replyTo: ActorRef[Event]) extends Command

  final case class CopyPart(sourceBucketName: String,
                            sourceKey: String,
                            targetBucketName: String,
                            targetKey: String,
                            uploadId: String,
                            partNumber: Int,
                            maybeRange: Option[ByteRange],
                            maybeSourceVersionId: Option[String],
                            replyTo: ActorRef[Event]) extends Command

  private final case object GetObject extends Command

  private final case class EventWrapper(event: Event) extends Command

  private case class PartInfo(uploadId: String, partNumber: Int)

}
