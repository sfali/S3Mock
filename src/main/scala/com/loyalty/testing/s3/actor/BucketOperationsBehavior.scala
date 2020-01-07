package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.actor.DeleteObjectsBehavior.{DeleteInput, Command => DeleteObjectsCommand}
import com.loyalty.testing.s3.actor.model.`object`.{CompleteUpload, DeleteObject, GetObject, GetObjectMeta, InitiateMultiPartUpload, PutObject, UploadPart, Command => ObjectCommand}
import com.loyalty.testing.s3.actor.model.bucket._
import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.repositories.collections.BucketNotEmptyException
import com.loyalty.testing.s3.repositories.model.Bucket

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class BucketOperationsBehavior private(context: ActorContext[Command],
                                       buffer: StashBuffer[Command],
                                       database: NitriteDatabase,
                                       objectOperationsActorRef: ActorRef[ShardingEnvelope[ObjectCommand]])
  extends AbstractBehavior[Command](context) {

  private val bucketId = UUID.fromString(context.self.path.name)
  context.setReceiveTimeout(5.minutes, Shutdown)
  context.self ! InitializeSnapshot

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case InitializeSnapshot =>
        val command =
          Try(database.findBucket(bucketId)) match {
            case Failure(ex: Throwable) =>
              context.log.error("database error", ex)
              DatabaseError
            case Success(maybeBucket) =>
              maybeBucket match {
                case Some(bucket) => BucketResult(bucket)
                case None =>
                  context.log.error(s"No such bucket: {}", bucketId)
                  NoSuchBucket
              }
          }
        context.self ! command
        Behaviors.same

      case NoSuchBucket => buffer.unstashAll(noSuchBucket)

      case DatabaseError => //TODO: handle properly
        Behaviors.same

      case BucketResult(bucket) => buffer.unstashAll(bucketOperation(bucket))

      case Shutdown => Behaviors.stopped

      case other =>
        buffer.stash(other)
        Behaviors.same

    }

  private def noSuchBucket: Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case CreateBucket(bucket, replyTo) =>
        val command =
          Try(database.createBucket(bucket)) match {
            case Failure(ex) =>
              context.log.error(s"unable to create bucket: $bucket", ex)
              // TODO: reply properly
              Shutdown
            case Success(_) => NewBucketCreated(bucket, replyTo)
          }
        context.self ! command
        Behaviors.same

      case NewBucketCreated(bucket, replyTo) =>
        context.self ! ReplyToSender(BucketInfo(bucket), replyTo)
        bucketOperation(bucket)

      case reply: CommandWithReply =>
        context.self ! ReplyToSender(NoSuchBucketExists(bucketId), reply.replyTo)
        Behaviors.same

      case ReplyToSender(reply, replyTo) =>
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Unhandled message in `noSuchBucket`: {}", other)
        Behaviors.unhandled
    }

  private def bucketOperation(bucket: Bucket): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case CreateBucket(_, replyTo) =>
        context.self ! ReplyToSender(BucketAlreadyExists(bucket), replyTo)
        Behaviors.same

      case SetBucketVersioning(versioningConfiguration, replyTo) =>
        val command =
          Try(database.setBucketVersioning(bucketId, versioningConfiguration)) match {
            case Failure(ex) =>
              context.log.error(s"unable to set bucket versioning: $bucket", ex)
              // TODO: reply properly
              Shutdown
            case Success(maybeBucket) =>
              maybeBucket match {
                case Some(bucket) => VersioningSet(bucket, replyTo)
                case None => ReplyToSender(NoSuchBucketExists(bucketId), replyTo)
              }
          }
        context.self ! command
        Behaviors.same

      case VersioningSet(updatedBucket, replyTo) =>
        context.self ! ReplyToSender(BucketInfo(updatedBucket), replyTo)
        bucketOperation(updatedBucket)

      case GetBucket(replyTo) =>
        context.self ! ReplyToSender(BucketInfo(bucket), replyTo)
        Behaviors.same

      case DeleteBucket(replyTo) =>
        val bucketName = bucket.bucketName
        val command =
          Try(database.deleteBucket(bucketName)) match {
            case Failure(_: BucketNotEmptyException) => ReplyToSender(BucketNotEmpty(bucketName), replyTo)
            case Failure(ex) =>
              context.log.error(s"unable to delete bucket: $bucketName", ex)
              Shutdown // TODO: reply properly
            case Success(_) => ReplyToSender(BucketDeleted, replyTo)
          }
        context.self ! command
        Behaviors.same

      case ListBucket(params, replyTo) =>
        val command =
          Try(listObjects(bucket.bucketName, params)(database, context.log)) match {
            case Failure(ex) =>
              context.log.error(s"Unable to get bucket content for bucket: ${bucket.bucketName} with params: $params", ex)
              // TODO: reply properly
              Shutdown
            case Success(contents) => ReplyToSender(ListBucketContent(contents), replyTo)
          }
        context.self ! command
        Behaviors.same

      case DeleteObjects(objects, verbose, replyTo) =>
        spawnDeleteObjectsActor ! DeleteInput(bucket, objects, verbose, replyTo)
        Behaviors.same

      case PutObjectWrapper(key, contentSource, copy, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), PutObject(bucket, key, contentSource, copy, replyTo))
        Behaviors.same

      case GetObjectMetaWrapper(key, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), GetObjectMeta(bucket, key, replyTo))
        Behaviors.same

      case GetObjectWrapper(key, maybeVersionId, maybeRange, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), GetObject(bucket, key, maybeVersionId, maybeRange, replyTo))
        Behaviors.same

      case DeleteObjectWrapper(key, maybeVersionId, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), DeleteObject(bucket, key, maybeVersionId, replyTo))
        Behaviors.same

      case InitiateMultiPartUploadWrapper(key, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), InitiateMultiPartUpload(bucket, key, replyTo))
        Behaviors.same

      case UploadPartWrapper(key, uploadId, partNumber, contentSource, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), UploadPart(bucket, key, uploadId, partNumber, contentSource, replyTo))
        Behaviors.same

      case CompleteUploadWrapper(key, uploadId, parts, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), CompleteUpload(bucket, key, uploadId, parts, replyTo))
        Behaviors.same

      case ReplyToSender(reply, replyTo) =>
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Unhandled message in `noSuchBucket`: {}", other)
        Behaviors.unhandled
    }

  private def spawnDeleteObjectsActor: ActorRef[DeleteObjectsCommand] =
    context.child(bucketId.toString) match {
      case Some(actorRef) => actorRef.unsafeUpcast[DeleteObjectsCommand]
      case None => context.spawn(DeleteObjectsBehavior(objectOperationsActorRef), bucketId.toString)
    }
}

object BucketOperationsBehavior {

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("BucketOperationsActor")

  def apply(database: NitriteDatabase,
            objectOperationsActorRef: ActorRef[ShardingEnvelope[ObjectCommand]]): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withStash(1000)(buffer => new BucketOperationsBehavior(context, buffer, database, objectOperationsActorRef))
    }

  def init(sharding: ClusterSharding,
           database: NitriteDatabase,
           objectOperationsActorRef: ActorRef[ShardingEnvelope[ObjectCommand]]): ActorRef[ShardingEnvelope[Command]] =
    sharding.init(Entity(TypeKey)(_ => BucketOperationsBehavior(database, objectOperationsActorRef)))
}
