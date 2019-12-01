package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.ObjectOperationsBehavior.ObjectProtocol
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.collections.NoSuchId
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import com.loyalty.testing.s3.request.BucketVersioning

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class ObjectOperationsBehavior(context: ActorContext[ObjectProtocol],
                               buffer: StashBuffer[ObjectProtocol],
                               objectIO: ObjectIO,
                               database: NitriteDatabase)
  extends AbstractBehavior(context) {

  import ObjectOperationsBehavior._

  private var versionIndex = 0
  private var objects: List[ObjectKey] = Nil
  private val objectId = UUID.fromString(context.self.path.name)
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
        val tuples = values.map(ok => (ok.index, ok.versionId))
        context.log.info("Current values: {} for {}", tuples, objectId)
        versionIndex = tuples.lastOption.map(_._1).getOrElse(0)
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
        versionIndex = if (BucketVersioning.Enabled == bucket.version) versionIndex + 1 else versionIndex
        context.pipeToSelf(objectIO.saveObject(bucket, key, objectId, versionIndex, contentSource)) {
          case Failure(ex) =>
            context.log.error(s"unable to save object: ${bucket.bucketName}/$key", ex)
            DatabaseError
          case Success(objectKey) => ObjectSaved(objectKey, replyTo)
        }
        Behaviors.same

      case DatabaseError =>
        Behaviors.same

      case ObjectSaved(objectKey, replyTo) =>
        context.pipeToSelf(database.createObject(objectKey)) {
          case Failure(ex) =>
            context.log.error(s"unable to save object: ${objectKey.bucketName}/${objectKey.key}", ex)
            DatabaseError // TODO: retry
          case Success(objectKey) =>
            // TODO: notify notification router
            ReplyToSender(ObjectInfo(objectKey), replyTo, Some(objectKey))
        }
        Behaviors.same

      case GetObject(bucket, _, maybeVersionId, maybeRange, replyTo) =>
        val maybeObjectKey = getObject(sanitizeVersionId(bucket, maybeVersionId))
        val command =
          if (maybeObjectKey.isEmpty) ReplyToSender(NoSuchKeyExists, replyTo)
          else {
            val objectKey = maybeObjectKey.get
            if (objectKey.deleteMarker.isEmpty) GetObjectData(objectKey, maybeRange, replyTo)
            else ReplyToSender(ObjectInfo(objectKey.copy(eTag = "", contentMd5 = "", contentLength = 0,
              deleteMarker = Some(true))), replyTo)
          }
        context.self ! command
        Behaviors.same

      case GetObjectData(objectKey, maybeRange, replyTo) =>
        val command =
          Try(objectIO.getObject(objectKey, maybeRange)) match {
            case Success((updatedObjectKey, source)) => ReplyToSender(ObjectContent(updatedObjectKey, source), replyTo)
            case Failure(ex) =>
              context.log.error("unable to download object", ex)
              DatabaseError // TODO: retry
          }
        context.self ! command
        Behaviors.same

      case DeleteObject(bucket, _, maybeVersionId, replyTo) =>
        val maybeObjectKey = getObject(sanitizeVersionId(bucket, maybeVersionId))
        if (maybeObjectKey.isEmpty) context.self ! ReplyToSender(NoSuchKeyExists, replyTo)
        else {
          val objectKey = maybeObjectKey.get
          val permanentDelete = objectKey.deleteMarker.isDefined
          context.pipeToSelf(database.deleteObject(objectId, maybeVersionId, permanentDelete)) {
            case Failure(ex) =>
              context.log.error(s"unable to delete object: ${objectKey.bucketName}/${objectKey.key}", ex)
              DatabaseError // TODO: retry
            case Success(_) => ObjectDeleted(objectKey.copy(deleteMarker = Some(permanentDelete)), replyTo)
          }
        }
        Behaviors.same

      case ObjectDeleted(objectKey, replyTo) =>
        val permanentDelete = objectKey.deleteMarker.getOrElse(false)
        val deleteInfo = DeleteInfo(permanentDelete, objectKey.version)
        val command =
          if (permanentDelete) {
            Try(objectIO.delete(objectKey)) match {
              case Failure(ex) =>
                context.log.error(s"unable to delete files: ${objectKey.bucketName}/${objectKey.key}", ex)
                DatabaseError // TODO: retry
              case Success(_) => ReplyToSender(deleteInfo, replyTo, Some(objectKey))
            }
          } else ReplyToSender(deleteInfo, replyTo, Some(objectKey))
        context.self ! command
        Behaviors.same

      case GetObjectMeta(replyTo) =>
        val maybeObjectKey = objects.find(om => om.id == objectId)
        val event = maybeObjectKey.map(ObjectInfo.apply).getOrElse(NoSuchKeyExists)
        context.self ! ReplyToSender(event, replyTo)
        Behaviors.same

      case ReplyToSender(reply, replyTo, maybeObjectMeta) =>
        objects =
          maybeObjectMeta match {
            case Some(objectKey) =>
              objectKey.version match {
                case BucketVersioning.Enabled => objects :+ objectKey
                case _ =>
                  val _objs = objects.filterNot(_.id == objectKey.id)
                  (_objs :+ objectKey).sortBy(_.index)
              }
            case None => objects
          }
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("unhandled message: {}", other)
        Behaviors.unhandled
    }

  private def getObject(maybeVersionId: Option[String]) =
    maybeVersionId match {
      case Some(versionId) => objects.filter(_.versionId == versionId).lastOption
      case None => objects.lastOption
    }
}

object ObjectOperationsBehavior {

  def apply(objectIO: ObjectIO,
            database: NitriteDatabase): Behavior[ObjectProtocol] =
    Behaviors.setup[ObjectProtocol] { context =>
      Behaviors.withStash[ObjectProtocol](1000) { buffer =>
        new ObjectOperationsBehavior(context, buffer, objectIO, database)
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
                                         maybeObjectKey: Option[ObjectKey] = None) extends ObjectProtocol

  private final case object InitializeSnapshot extends ObjectProtocol

  private final case class ObjectResult(values: List[ObjectKey]) extends ObjectProtocol

  private final case class ObjectSaved(objectKey: ObjectKey, replyTo: ActorRef[Event]) extends ObjectProtocolWithReply

  final case class PutObject(bucket: Bucket,
                             key: String,
                             contentSource: Source[ByteString, _],
                             replyTo: ActorRef[Event]) extends ObjectInput

  final case class GetObjectMeta(replyTo: ActorRef[Event]) extends ObjectProtocolWithReply

  final case class GetObject(bucket: Bucket,
                             key: String,
                             maybeVersionId: Option[String],
                             maybeRange: Option[ByteRange],
                             replyTo: ActorRef[Event]) extends ObjectInput

  private final case class GetObjectData(objectKey: ObjectKey,
                                         maybeRange: Option[ByteRange],
                                         replyTo: ActorRef[Event]) extends ObjectProtocolWithReply

  final case class DeleteObject(bucket: Bucket,
                                key: String,
                                maybeVersionId: Option[String],
                                replyTo: ActorRef[Event]) extends ObjectInput

  private final case class ObjectDeleted(objectKey: ObjectKey,
                                         replyTo: ActorRef[Event]) extends ObjectProtocolWithReply

  private def sanitizeVersionId(bucket: Bucket, maybeVersionId: Option[String]): Option[String] =
    if (BucketVersioning.NotExists == bucket.version && maybeVersionId.isDefined) {
      // if version id provided and versioning doesn't exists then set some dummy value
      // so that it results in NoSuckKey
      Some(UUID.randomUUID().toString)
    } else maybeVersionId
}
