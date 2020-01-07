package com.loyalty.testing.s3.actor

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.loyalty.testing.s3.actor.DeleteObjectsBehavior.Command
import com.loyalty.testing.s3.actor.model.`object`.{DeleteObject, Command => ObjectCommand}
import com.loyalty.testing.s3.actor.model.{DeleteObjectsResult, Event, NoSuchKeyExists, ObjectInfo}
import com.loyalty.testing.s3.entityId
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectStatus}
import com.loyalty.testing.s3.request.ObjectIdentifier
import com.loyalty.testing.s3.response.{DeleteError, DeleteResult, DeletedObject, NoSuchKeyResponse}

import scala.concurrent.duration._

class DeleteObjectsBehavior(context: ActorContext[Command],
                            timer: TimerScheduler[Command],
                            objectOperationsActorRef: ActorRef[ShardingEnvelope[ObjectCommand]])
  extends AbstractBehavior[Command](context) {

  import DeleteObjectsBehavior._
  import ObjectStatus._

  private val eventResponseWrapper = context.messageAdapter[Event](EventWrapper.apply)
  private var successes = Map.empty[String, DeletedObject]
  private var errors = Map.empty[String, DeleteError]
  private var responseReceived = 0
  context.setReceiveTimeout(3.minutes, Shutdown)

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case DeleteInput(bucket, objects, verbose, replyTo) =>
        context.self ! ExecuteDelete
        deleteObjects(bucket, objects, verbose, replyTo)

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("unhandled message: {}", other.getClass.getSimpleName)
        Behaviors.same
    }

  private def deleteObjects(bucket: Bucket,
                            objects: List[ObjectIdentifier],
                            verbose: Boolean,
                            replyTo: ActorRef[Event]): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case ExecuteDelete =>
        objects
          .foreach {
            objectIdentifier =>
              val key = objectIdentifier.key
              val maybeVersionId = objectIdentifier.versionId
              val command = DeleteObject(bucket, key, maybeVersionId, eventResponseWrapper)
              objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), command)
          }
        timer.startTimerWithFixedDelay("verify-count", CheckIfAllResponseReceived, 1.second)
        Behaviors.same

      case EventWrapper(ObjectInfo(objectKey)) =>
        responseReceived += 1
        val actualVersionId = objectKey.actualVersionId
        val status = objectKey.status
        val isDeleteMarker = status == DeleteMarker || status == DeleteMarkerDeleted

        val versionId =
          if (status == DeleteMarker) None
          else actualVersionId

        val deleteMarker =
          if (isDeleteMarker) Some(true)
          else None

        val deleteMarkerVersionId =
          if (isDeleteMarker) actualVersionId
          else None

        val deletedObject = DeletedObject(
          key = Some(objectKey.key),
          versionId = versionId,
          deleteMarker = deleteMarker,
          deleteMarkerVersionId = deleteMarkerVersionId)
        successes += (objectKey.key -> deletedObject)
        Behaviors.same

      case EventWrapper(NoSuchKeyExists(bucketName, key)) =>
        responseReceived += 1
        val error = NoSuchKeyResponse(bucketName, key)
        errors += (key -> DeleteError(key, error.code, error.message))
        Behaviors.same

      case EventWrapper(event) =>
        responseReceived += 1
        /*val error = InternalServiceResponse(bucket.bucketName)
        responseData += (key -> DeleteError(key, error.code, error.message))*/
        context.log.warn("Invalid event received, event={}, bucket_name={}", event, bucket.bucketName)
        Behaviors.same

      case CheckIfAllResponseReceived =>
        if (responseReceived == objects.length) {
          timer.cancel("verify-count")
          val result =
            if (verbose) DeleteResult(successes.values.toList, errors.values.toList)
            else DeleteResult(errors = errors.values.toList)
          replyTo ! DeleteObjectsResult(result)
          timer.startSingleTimer(Shutdown, 500.milliseconds)
        }
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("unhandled message: {}", other.getClass.getSimpleName)
        Behaviors.unhandled
    }
}

object DeleteObjectsBehavior {

  def apply(objectOperationsActorRef: ActorRef[ShardingEnvelope[ObjectCommand]]): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withTimers[Command] { timer =>
        new DeleteObjectsBehavior(context, timer, objectOperationsActorRef)
      }
    }

  sealed trait Command

  private final case object Shutdown extends Command

  final case class DeleteInput(bucket: Bucket,
                               objects: List[ObjectIdentifier],
                               verbose: Boolean,
                               replyTo: ActorRef[Event]) extends Command

  private final case object CheckIfAllResponseReceived extends Command

  private final case object ExecuteDelete extends Command

  private final case class EventWrapper(event: Event) extends Command

}
