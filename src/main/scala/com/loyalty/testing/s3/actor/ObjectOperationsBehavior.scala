package com.loyalty.testing.s3.actor

import java.nio.file.Path
import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.ObjectOperationsBehavior.ObjectProtocol
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.collections.NoSuchId
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.ObjectMeta
import com.loyalty.testing.s3.streams.FileStream

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class ObjectOperationsBehavior(context: ActorContext[ObjectProtocol],
                               buffer: StashBuffer[ObjectProtocol],
                               dataPath: Path,
                               database: NitriteDatabase)
  extends AbstractBehavior(context) {

  import ObjectOperationsBehavior._

  private implicit val system: ActorSystem[Nothing] = context.system
  private implicit val ec: ExecutionContextExecutor = context.executionContext
  private var versionIndex = 0
  private var objects: List[ObjectMeta] = Nil
  private val objectId = UUID.fromString(context.self.path.name)
  private val fileStream = FileStream()
  context.setReceiveTimeout(5.minutes, Shutdown)
  context.self ! InitializeSnapshot

  override def onMessage(msg: ObjectProtocol): Behavior[ObjectProtocol] =
    msg match {
      case InitializeSnapshot =>
        context.pipeToSelf(database.getAllObjects(objectId)) {
          case Failure(NoSuchId(_)) => ObjectResult(Nil)
          case Failure(ex) =>
            context.log.error(s"unable to load snapshot: $objectId", ex)
            // TODO: reply properly
            Shutdown
          case Success(values) => ObjectResult(values)
        }
        Behaviors.same

      case ObjectResult(values) =>
        val tuples = values.map(_.result).map(result => (result.index, result.maybeVersionId))
        context.log.info("Current values: {}", tuples)
        versionIndex = tuples.headOption.map(_._1).getOrElse(0)
        objects = values
        buffer.unstashAll(objectOperations)

      case Shutdown => Behaviors.stopped

      case other =>
        buffer.stash(other)
        Behaviors.same
    }

  private def objectOperations: Behavior[ObjectProtocol] =
    Behaviors.receiveMessagePartial {
      case protocol: ObjectInput if protocol.objectId != objectId =>
        context.self ! ReplyToSender(InvalidAccess, protocol.replyTo)
        Behaviors.same

      case PutObject(bucket, key, contentSource, replyTo) =>
        context.pipeToSelf(saveObject(fileStream, key, bucket.bucketPath(dataPath), bucket.version, contentSource)) {
          case Failure(ex) =>
            context.log.error(s"unable to save object: ${bucket.bucketName}/$key", ex)
            DatabaseError
          case Success(objectMeta) => ObjectSaved(bucket, key, objectMeta, replyTo)
        }
        Behaviors.same

      case DatabaseError =>
        Behaviors.same

      case ObjectSaved(bucket, key, objectMeta, replyTo) =>
        versionIndex =
          bucket.version match {
            case Some(value) => if (value == BucketVersioning.Enabled) versionIndex + 1 else versionIndex
            case None => versionIndex
          }
        context.pipeToSelf(database.createObject(bucket, key, objectMeta.result, versionIndex)) {
          case Failure(ex) =>
            context.log.error(s"unable to save object: ${bucket.bucketName}/$key", ex)
            DatabaseError // TODO: retry
          case Success(value) =>
            val updateMeta = objectMeta.copy(lastModifiedDate = value.lastModifiedTime.toLocalDateTime, id = value.id)
            ReplyToSender(ObjectInfo(updateMeta), replyTo, Some(updateMeta))
        }
        Behaviors.same

      case GetObjectMeta(replyTo) =>
        val maybeObjectMeta = objects.find(om => om.id == objectId)
        val event = maybeObjectMeta.map(ObjectInfo.apply).getOrElse(NoSuchKeyExists)
        context.self ! ReplyToSender(event, replyTo)
        Behaviors.same

      case ReplyToSender(reply, replyTo, maybeObjectMeta) =>
        objects = maybeObjectMeta.map(objects :+ _).getOrElse(objects)
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("unhandled message: {}", other)
        Behaviors.unhandled
    }
}

object ObjectOperationsBehavior {

  def apply(dataPath: Path,
            database: NitriteDatabase): Behavior[ObjectProtocol] =
    Behaviors.setup[ObjectProtocol] { context =>
      Behaviors.withStash[ObjectProtocol](1000) { buffer =>
        new ObjectOperationsBehavior(context, buffer, dataPath, database)
      }
    }

  sealed trait ObjectProtocol

  sealed trait ObjectProtocolWithReply extends ObjectProtocol {
    val replyTo: ActorRef[Event]
  }

  sealed trait ObjectInput extends ObjectProtocolWithReply {
    val bucket: Bucket
    val key: String
    lazy val objectId: UUID = createObjectId(bucket.bucketName, key)
  }

  private final case object Shutdown extends ObjectProtocol

  private final case object DatabaseError extends ObjectProtocol

  private final case class ReplyToSender(reply: Event,
                                         replyTo: ActorRef[Event],
                                         maybeObjectMeta: Option[ObjectMeta] = None) extends ObjectProtocol

  private final case object InitializeSnapshot extends ObjectProtocol

  private final case class ObjectResult(values: List[ObjectMeta]) extends ObjectProtocol

  private final case class ObjectSaved(bucket: Bucket,
                                       key: String,
                                       objectMeta: ObjectMeta,
                                       replyTo: ActorRef[Event]) extends ObjectInput

  final case class PutObject(bucket: Bucket,
                             key: String,
                             contentSource: Source[ByteString, _],
                             replyTo: ActorRef[Event]) extends ObjectInput

  final case class GetObjectMeta(replyTo: ActorRef[Event]) extends ObjectProtocolWithReply

}
