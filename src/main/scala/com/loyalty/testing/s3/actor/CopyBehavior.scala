package com.loyalty.testing.s3.actor

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.Command
import com.loyalty.testing.s3.actor.model.bucket.{GetObjectWrapper, PutObjectWrapper, Command => BucketCommand}
import com.loyalty.testing.s3.actor.model.{CopyObjectInfo, Event, ObjectContent, ObjectInfo}
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
        copyOperation(sourceBucketName, sourceKey, targetBucketName, targetKey, maybeSourceVersionId, replyTo)

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Invalid command in onMessage: {}", other)
        Behaviors.unhandled
    }

  private def copyOperation(sourceBucketName: String,
                            sourceKey: String,
                            targetBucketName: String,
                            targetKey: String,
                            maybeSourceVersionId: Option[String],
                            replyTo: ActorRef[Event]): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case GetObject =>
        context.log.info(
          """Copy Object: source_bucket_name={}, source_key={},  target_bucket_name={},
            | target_key={}""".stripMargin.replaceNewLine, sourceBucketName, sourceKey, targetBucketName, targetKey)
        getBucketActor(sourceBucketName) ! GetObjectWrapper(sourceKey, maybeSourceVersionId, None, eventResponseWrapper)
        Behaviors.same

      case EventWrapper(ObjectContent(objectKey, content)) =>
        sourceVersionId = objectKey.actualVersionId
        context.log.info(
          """Got source content: source_bucket_name={}, source_key={},  target_bucket_name={},
            | target_key={}, source_version_id={}""".stripMargin.replaceNewLine, sourceBucketName, sourceKey,
          targetBucketName, targetKey, sourceVersionId)
        getBucketActor(targetBucketName) ! PutObjectWrapper(targetKey, content, eventResponseWrapper)
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

  private final case object GetObject extends Command

  private final case class EventWrapper(event: Event) extends Command

}
