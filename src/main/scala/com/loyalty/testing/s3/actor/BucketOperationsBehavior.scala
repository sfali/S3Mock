package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.{BucketAlreadyExists, BucketInfo, NoSuchBucketExists, NotificationsCreated, NotificationsInfo}
import com.loyalty.testing.s3.actor.model.`object`.{CompleteUpload, DeleteObject, GetObject, GetObjectMeta, InitiateMultiPartUpload, PutObject, UploadPart, Command => ObjectCommand}
import com.loyalty.testing.s3.actor.model.bucket._
import com.loyalty.testing.s3.repositories.collections.NoSuckBucketException
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BucketOperationsBehavior private(context: ActorContext[Command],
                                       buffer: StashBuffer[Command],
                                       objectIO: ObjectIO,
                                       database: NitriteDatabase)
  extends AbstractBehavior[Command](context) {

  private implicit val ec: ExecutionContext = context.system.executionContext
  private val bucketId = UUID.fromString(context.self.path.name)
  context.setReceiveTimeout(5.minutes, Shutdown)
  context.self ! InitializeSnapshot

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case InitializeSnapshot =>
        context.pipeToSelf(database.getBucket(bucketId)) {
          case Failure(_: NoSuckBucketException) =>
            context.log.error(s"No such bucket: {}", bucketId)
            NoSuchBucket
          case Failure(ex: Throwable) =>
            context.log.error("database error", ex)
            DatabaseError
          case Success(bucket) => BucketResult(bucket)
        }
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
        context.pipeToSelf(database.createBucket(bucket)) {
          case Failure(ex) =>
            context.log.error(s"unable to create bucket: $bucket", ex)
            // TODO: reply properly
            Shutdown

          case Success(bucket) => NewBucketCreated(bucket, replyTo)
        }
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
        context.pipeToSelf(database.setBucketVersioning(bucketId, bucket.bucketName, versioningConfiguration)) {
          case Failure(ex) =>
            context.log.error(s"unable to set bucket versioning: $bucket", ex)
            // TODO: reply properly
            Shutdown
          case Success(bucket) => VersioningSet(bucket, replyTo)
        }
        Behaviors.same

      case VersioningSet(updatedBucket, replyTo) =>
        context.self ! ReplyToSender(BucketInfo(updatedBucket), replyTo)
        bucketOperation(updatedBucket)

      case CreateBucketNotifications(notifications, replyTo) =>
        context.pipeToSelf(database.setBucketNotifications(notifications)) {
          case Failure(ex) =>
            context.log.error(s"unable to create bucket notifications: $bucket", ex)
            // TODO: reply properly
            Shutdown
          case Success(_) => ReplyToSender(NotificationsCreated, replyTo)
        }
        Behaviors.same

      case GetBucket(replyTo) =>
        context.self ! ReplyToSender(BucketInfo(bucket), replyTo)
        Behaviors.same

      case GetBucketNotifications(replyTo) =>
        context.pipeToSelf(database.getBucketNotifications(bucket.bucketName)) {
          case Failure(ex) =>
            context.log.error(s"unable to create bucket notifications: $bucket", ex)
            // TODO: reply properly
            Shutdown
          case Success(notifications) => ReplyToSender(NotificationsInfo(notifications), replyTo)
        }
        Behaviors.same

      case PutObjectWrapper(key, contentSource, replyTo) =>
        objectActor(bucket, key) ! PutObject(bucket, key, contentSource, replyTo)
        Behaviors.same

      case GetObjectMetaWrapper(key, replyTo) =>
        objectActor(bucket, key) ! GetObjectMeta(bucket, key, replyTo)
        Behaviors.same

      case GetObjectWrapper(key, maybeVersionId, maybeRange, replyTo) =>
        objectActor(bucket, key) ! GetObject(bucket, key, maybeVersionId, maybeRange, replyTo)
        Behaviors.same

      case DeleteObjectWrapper(key, maybeVersionId, replyTo) =>
        objectActor(bucket, key) ! DeleteObject(bucket, key, maybeVersionId, replyTo)
        Behaviors.same

      case InitiateMultiPartUploadWrapper(key, replyTo) =>
        objectActor(bucket, key) ! InitiateMultiPartUpload(bucket, key, replyTo)
        Behaviors.same

      case UploadPartWrapper(key, uploadId, partNumber, contentSource, replyTo) =>
        objectActor(bucket, key) ! UploadPart(bucket, key, uploadId, partNumber, contentSource, replyTo)
        Behaviors.same

      case CompleteUploadWrapper(key, uploadId, parts, replyTo) =>
        objectActor(bucket, key) ! CompleteUpload(bucket, key, uploadId, parts, replyTo)
        Behaviors.same

      case ReplyToSender(reply, replyTo) =>
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Unhandled message in `noSuchBucket`: {}", other)
        Behaviors.unhandled
    }

  private def objectActor(bucket: Bucket, key: String): ActorRef[ObjectCommand] = {
    val id = createObjectId(bucket.bucketName, key).toString
    context.child(id) match {
      case Some(behavior) => behavior.unsafeUpcast[ObjectCommand]
      case None => context.spawn(ObjectOperationsBehavior(objectIO, database), id)
    }
  }

}

object BucketOperationsBehavior {

  def apply(objectIO: ObjectIO, database: NitriteDatabase): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withStash(1000)(buffer => new BucketOperationsBehavior(context, buffer, objectIO, database))
    }
}
