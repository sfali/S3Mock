package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.BucketOperationsBehavior.BucketProtocol
import com.loyalty.testing.s3.actor.ObjectOperationsBehavior._
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.VersioningConfiguration
import com.loyalty.testing.s3.response.NoSuchBucketException

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BucketOperationsBehavior private(context: ActorContext[BucketProtocol],
                                       buffer: StashBuffer[BucketProtocol],
                                       objectIO: ObjectIO,
                                       database: NitriteDatabase)
  extends AbstractBehavior[BucketProtocol](context) {

  import BucketOperationsBehavior._

  private val bucketId = UUID.fromString(context.self.path.name)
  context.setReceiveTimeout(5.minutes, Shutdown)
  context.self ! InitializeSnapshot

  override def onMessage(msg: BucketProtocol): Behavior[BucketProtocol] =
    msg match {
      case InitializeSnapshot =>
        context.pipeToSelf(database.getBucket(bucketId)) {
          case Failure(_: NoSuchBucketException) =>
            // context.log.error(s"No such bucket: $bucketId", ex)
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

  private def noSuchBucket: Behavior[BucketProtocol] =
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

      case reply: BucketProtocolWithReply =>
        context.self ! ReplyToSender(NoSuchBucketExists, reply.replyTo)
        Behaviors.same

      case ReplyToSender(reply, replyTo) =>
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Unhandled message in `noSuchBucket`: {}", other)
        Behaviors.unhandled
    }

  private def bucketOperation(bucket: Bucket): Behavior[BucketProtocol] =
    Behaviors.receiveMessagePartial {
      case CreateBucket(_, replyTo) =>
        context.self ! ReplyToSender(BucketAlreadyExists(bucket), replyTo)
        Behaviors.same

      case SetBucketVersioning(versioningConfiguration, replyTo) =>
        context.pipeToSelf(database.setBucketVersioning(bucketId, versioningConfiguration)) {
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
        objectActor(bucket, key) ! GetObjectMeta(replyTo)
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

      case ReplyToSender(reply, replyTo) =>
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("Unhandled message in `noSuchBucket`: {}", other)
        Behaviors.unhandled
    }

  private def objectActor(bucket: Bucket, key: String): ActorRef[ObjectProtocol] = {
    val id = createObjectId(bucket.bucketName, key).toString
    context.child(id) match {
      case Some(behavior) => behavior.unsafeUpcast[ObjectProtocol]
      case None => context.spawn(ObjectOperationsBehavior(objectIO, database), id)
    }
  }

}

object BucketOperationsBehavior {

  def apply(objectIO: ObjectIO, database: NitriteDatabase): Behavior[BucketProtocol] =
    Behaviors.setup { context =>
      Behaviors.withStash(1000)(buffer => new BucketOperationsBehavior(context, buffer, objectIO, database))
    }

  sealed trait BucketProtocol

  sealed trait BucketProtocolWithReply extends BucketProtocol {
    val replyTo: ActorRef[Event]
  }

  private final case object Shutdown extends BucketProtocol

  private final case object InitializeSnapshot extends BucketProtocol

  private final case object NoSuchBucket extends BucketProtocol

  private final case object DatabaseError extends BucketProtocol

  private final case class ReplyToSender(reply: Event, replyTo: ActorRef[Event]) extends BucketProtocol

  private case class NewBucketCreated(bucket: Bucket, replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  private final case class BucketResult(bucket: Bucket) extends BucketProtocol

  private final case class VersioningSet(updatedBucket: Bucket, replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class CreateBucket(bucket: Bucket, replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class GetBucket(replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class GetBucketNotifications(replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class SetBucketVersioning(versioningConfiguration: VersioningConfiguration,
                                       replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class CreateBucketNotifications(notifications: List[Notification],
                                             replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class PutObjectWrapper(key: String,
                                    contentSource: Source[ByteString, _],
                                    replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class GetObjectMetaWrapper(key: String, replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class GetObjectWrapper(key: String,
                                    maybeVersionId: Option[String] = None,
                                    maybeRange: Option[ByteRange] = None,
                                    replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class DeleteObjectWrapper(key: String,
                                       maybeVersionId: Option[String] = None,
                                       replyTo: ActorRef[Event]) extends BucketProtocolWithReply

  final case class InitiateMultiPartUploadWrapper(key: String,
                                                  replyTo: ActorRef[Event]) extends BucketProtocolWithReply

}
