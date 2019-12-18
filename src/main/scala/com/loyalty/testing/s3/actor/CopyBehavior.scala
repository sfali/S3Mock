package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.http.scaladsl.model.headers.ByteRange
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.Command
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.actor.model.bucket.{GetObjectWrapper, PutObjectWrapper, UploadPartWrapper, Command => BucketCommand}

import scala.concurrent.duration._

class CopyBehavior(context: ActorContext[Command],
                   bucketOperationsActorRef: ActorRef[ShardingEnvelope[BucketCommand]])
  extends AbstractBehavior[Command](context) {

  import CopyBehavior._

  context.setReceiveTimeout(2.minutes, Shutdown)

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case Copy(sourceBucketName, sourceKey, targetBucketName, targetKey, maybeSourceVersionId, replyTo) =>
        context.log.info(
          "Copy object: source_bucket_name={}, source_key={},  target_bucket_name={}, target_key={}",
          sourceBucketName, sourceKey, targetBucketName, targetKey)
        val behavior = copyOperationBehavior(
          bucketOperationsActorRef,
          S3Location(sourceBucketName, sourceKey),
          S3Location(targetBucketName, targetKey),
          None,
          None,
          maybeSourceVersionId,
          replyTo
        )
        val actorRef = context.spawn(behavior, UUID.randomUUID().toString)
        actorRef ! GetObject
        Behaviors.same

      case CopyPart(sourceBucketName, sourceKey, targetBucketName, targetKey, uploadId, partNumber, range,
      maybeSourceVersionId, replyTo) =>
        context.log.info(
          "Copy part: source_bucket_name={}, source_key={},  target_bucket_name={}, target_key={}",
          sourceBucketName, sourceKey, targetBucketName, targetKey)
        val behavior = copyOperationBehavior(
          bucketOperationsActorRef,
          S3Location(sourceBucketName, sourceKey),
          S3Location(targetBucketName, targetKey),
          Some(PartInfo(uploadId, partNumber)),
          range,
          maybeSourceVersionId,
          replyTo
        )
        val actorRef = context.spawn(behavior, UUID.randomUUID().toString)
        actorRef ! GetObject
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Invalid command in onMessage: {}", other)
        Behaviors.unhandled
    }
}

object CopyBehavior {

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("CopyOperations")

  def apply(bucketOperationsActorRef: ActorRef[ShardingEnvelope[BucketCommand]]): Behavior[Command] =
    Behaviors.setup[Command](context => new CopyBehavior(context, bucketOperationsActorRef))

  sealed trait Command extends CborSerializable

  private sealed trait OperationCommand

  private final case object Shutdown extends Command with OperationCommand

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

  private final case object GetObject extends OperationCommand

  private final case class EventWrapper(event: Event) extends OperationCommand

  private case class S3Location(bucketName: String, key: String)

  private case class PartInfo(uploadId: String, partNumber: Int)

  private def copyOperationBehavior(bucketOperationsActorRef: ActorRef[ShardingEnvelope[BucketCommand]],
                                    source: S3Location,
                                    target: S3Location,
                                    partInfo: Option[PartInfo],
                                    maybeRange: Option[ByteRange],
                                    maybeSourceVersionId: Option[String],
                                    replyTo: ActorRef[Event]): Behavior[OperationCommand] =
    Behaviors.setup { context =>

      context.setReceiveTimeout(1.minute, Shutdown)
      val eventResponseWrapper = context.messageAdapter[Event](EventWrapper.apply)
      var sourceVersionId: Option[String] = None

      Behaviors.receiveMessagePartial {
        case GetObject =>
          bucketOperationsActorRef ! ShardingEnvelope(source.bucketName.toUUID.toString,
            GetObjectWrapper(source.key, maybeSourceVersionId, maybeRange, eventResponseWrapper))
          Behaviors.same

        case EventWrapper(ObjectContent(objectKey, content)) if partInfo.isDefined =>
          sourceVersionId = objectKey.actualVersionId
          val uploadId = partInfo.get.uploadId
          val partNumber = partInfo.get.partNumber
          context.log.info(
            """Copy part: Got source content: source_bucket_name={}, source_key={},  target_bucket_name={},
              | target_key={}, source_version_id={}, upload_id={}, part_number={}""".stripMargin.replaceNewLine,
            source.bucketName, source.key, target.bucketName, target.key, sourceVersionId, uploadId, partNumber)
          bucketOperationsActorRef ! ShardingEnvelope(target.bucketName.toUUID.toString,
            UploadPartWrapper(target.key, uploadId, partNumber, content, eventResponseWrapper))
          Behaviors.same

        case EventWrapper(ObjectContent(objectKey, content)) =>
          sourceVersionId = objectKey.actualVersionId
          context.log.info(
            """Copy object: Got source content: source_bucket_name={}, source_key={},  target_bucket_name={},
              | target_key={}, source_version_id={}, range={}""".stripMargin.replaceNewLine, source.bucketName, source.key,
            target.bucketName, target.key, sourceVersionId, maybeRange)
          bucketOperationsActorRef ! ShardingEnvelope(target.bucketName.toUUID.toString,
            PutObjectWrapper(target.key, content, eventResponseWrapper))
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
              | target_key={}""".stripMargin.replaceNewLine, event, source.bucketName, source.key, target.bucketName, target.key)
          replyTo ! event
          Behaviors.same

        case Shutdown => Behaviors.stopped

        case other =>
          context.log.warn("Invalid command: {}", other)
          Behaviors.unhandled
      }
    }

}
