package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.actor.model.`object`.{CompleteUpload, DeleteObject, GetObject, GetObjectMeta, InitiateMultiPartUpload, PutObject, UploadPart, Command => ObjectCommand}
import com.loyalty.testing.s3.actor.model.bucket._
import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.repositories.collections.NoSuckBucketException
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
          Try(database.getBucket(bucketId)) match {
            case Failure(_: NoSuckBucketException) =>
              context.log.error(s"No such bucket: {}", bucketId)
              NoSuchBucket
            case Failure(ex: Throwable) =>
              context.log.error("database error", ex)
              DatabaseError
            case Success(bucket) => BucketResult(bucket)
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
            case Success(value) => NewBucketCreated(bucket, replyTo)
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
          Try(database.setBucketVersioning(bucketId, bucket.bucketName, versioningConfiguration)) match {
            case Failure(ex) =>
              context.log.error(s"unable to set bucket versioning: $bucket", ex)
              // TODO: reply properly
              Shutdown
            case Success(bucket) => VersioningSet(bucket, replyTo)
          }
        context.self ! command
        Behaviors.same

      case VersioningSet(updatedBucket, replyTo) =>
        context.self ! ReplyToSender(BucketInfo(updatedBucket), replyTo)
        bucketOperation(updatedBucket)

      case GetBucket(replyTo) =>
        context.self ! ReplyToSender(BucketInfo(bucket), replyTo)
        Behaviors.same

      case ListBucket(params, replyTo) =>
        val command =
          Try(listBucket(bucket.bucketName, params)(database, context.log)) match {
            case Failure(ex) =>
              context.log.error(s"Unable to get bucket content for bucket: ${bucket.bucketName} with params: $params", ex)
              // TODO: reply properly
              Shutdown
            case Success(contents) => ReplyToSender(ListBucketContent(contents), replyTo)
          }
        context.self ! command
        Behaviors.same

      case PutObjectWrapper(key, contentSource, replyTo) =>
        objectOperationsActorRef ! ShardingEnvelope(entityId(bucket, key), PutObject(bucket, key, contentSource, replyTo))
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

  private def entityId(bucket: Bucket, key: String) = createObjectId(bucket.bucketName, key).toString
}

object BucketOperationsBehavior {

  def apply(database: NitriteDatabase,
            objectOperationsActorRef: ActorRef[ShardingEnvelope[ObjectCommand]]): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withStash(1000)(buffer => new BucketOperationsBehavior(context, buffer, database, objectOperationsActorRef))
    }
}
