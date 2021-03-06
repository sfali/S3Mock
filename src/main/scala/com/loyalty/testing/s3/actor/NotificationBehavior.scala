package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import com.loyalty.testing.s3.actor.NotificationBehavior.Command
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationData, _}
import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.service.NotificationService
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class NotificationBehavior(context: ActorContext[Command],
                           buffer: StashBuffer[Command],
                           database: NitriteDatabase,
                           notificationService: NotificationService)
  extends AbstractBehavior[Command](context) {

  import DestinationType._
  import NotificationBehavior._

  private val bucketId = UUID.fromString(context.self.path.name)
  context.setReceiveTimeout(1.minute, Shutdown)
  context.self ! Initialize

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case Initialize =>
        val command =
          Try(database.findBucket(bucketId)) match {
            case Failure(ex) =>
              context.log.error("Unable to get bucket", ex)
              NoSuchBucket // TODO: retry
            case Success(maybeBucket) =>
              maybeBucket match {
                case Some(bucket) => BucketResult(bucket)
                case None => NoSuchBucket
              }
          }
        context.self ! command
        Behaviors.same

      case NoSuchBucket => buffer.unstashAll(noSuchBucket)

      case BucketResult(bucket) =>
        Try(database.getBucketNotifications(bucket.bucketName)) match {
          case Failure(ex) =>
            context.log.error("Unable to get notifications", ex)
            //TODO: retry
            Behaviors.same
          case Success(notifications) => buffer.unstashAll(bucketOperation(bucket.bucketName, notifications))
        }

      case Shutdown => Behaviors.stopped

      case other =>
        buffer.stash(other)
        Behaviors.same
    }

  private def noSuchBucket: Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case reply: CommandWithReply =>
        reply.replyTo ! NoSuchBucketExists(bucketId)
        Behaviors.same

      case Shutdown => Behaviors.stopped
    }

  private def bucketOperation(bucketName: String, notifications: List[Notification]): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case CreateBucketNotifications(notifications, replyTo) =>
        context.log.info("Setting bucket notifications, notification={}, bucket_name={}", notifications, bucketName)
        val command =
          notifications match {
            case Nil =>
              Try(database.deleteNotifications(bucketName)) match {
                case Failure(ex) =>
                  context.log.error(s"Unable to delete notifications: bucket_name=$bucketName", ex)
                  Standby // TODO: retry
                case Success(_) => UpdateNotifications(notifications, replyTo)
              }
            case _ =>
              Try(database.setBucketNotifications(notifications)) match {
                case Failure(ex) =>
                  context.log.error(s"Unable to create notifications: bucket_name=$bucketName", ex)
                  Standby // TODO: retry
                case Success(_) => UpdateNotifications(notifications, replyTo)
              }
          }
        context.self ! command
        Behaviors.same

      case UpdateNotifications(notifications, replyTo) =>
        replyTo ! NotificationsCreated
        bucketOperation(bucketName, notifications)

      case GetBucketNotifications(replyTo) =>
        replyTo ! NotificationsInfo(notifications)
        Behaviors.same

      case SendNotification(notificationData) =>
        val commands = notifications.map(parseNotification(notificationData, context.log))
        commands.foreach(command => context.self ! command)
        Behaviors.same

      case SendNotificationToDestination(destinationType, destinationName, message) if destinationType == Sqs =>
        context.pipeToSelf(notificationService.sendSqsMessage(message, destinationName)) {
          case Failure(ex) =>
            context.log.warn(s"Unable to send message, message=$message, queue_name=$destinationName", ex)
            Standby // TODO: based on error retry or abort
          case Success(_) => Standby
        }
        Behaviors.same

      case SendNotificationToDestination(destinationType, destinationName, message) if destinationType == Sns =>
        context.pipeToSelf(notificationService.sendSnsMessage(message, destinationName)) {
          case Failure(ex) =>
            context.log.warn(s"Unable to send message, message=$message, topic_arn=$destinationName", ex)
            Standby // TODO: based on error retry or abort
          case Success(_) => Standby
        }
        Behaviors.same

      case Standby => Behaviors.same

      case Shutdown => Behaviors.stopped
    }

  private def parseNotification(notificationData: NotificationData,
                                log: Logger)(notification: Notification): Command = {
    val key = notificationData.key
    val prefix = notification.prefix
    val suffix = notification.suffix
    val prefixMatch = prefix.exists(key.startsWith) || prefix.isEmpty
    val suffixMatch = suffix.exists(key.endsWith) || suffix.isEmpty
    if (notification.notificationType.isValidOperation(notificationData.operation) || (prefixMatch && suffixMatch)) {
      val message = generateMessage(notificationData, notification)
      log.info("Sending message `{}` to queue: {}", message, notification.destinationName)
      SendNotificationToDestination(notification.destinationType, notification.destinationName, message)
    } else {
      log.warn("Not sending notification, data={}, notification={}", notificationData, notification)
      Standby
    }
  }
}

object NotificationBehavior {

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("NotificationActor")

  def apply(database: NitriteDatabase,
            notificationService: NotificationService): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withStash[Command](20) { buffer =>
        new NotificationBehavior(context, buffer, database, notificationService)
      }
    }

  def init(sharding: ClusterSharding,
           database: NitriteDatabase,
           notificationService: NotificationService): ActorRef[ShardingEnvelope[Command]] =
    sharding.init(Entity(TypeKey)(_ => NotificationBehavior(database, notificationService)))

  sealed trait Command extends CborSerializable

  sealed trait CommandWithReply extends Command {
    val replyTo: ActorRef[Event]
  }

  private case object Initialize extends Command

  private case object NoSuchBucket extends Command

  private case class BucketResult(bucket: Bucket) extends Command

  final case class CreateBucketNotifications(notifications: List[Notification],
                                             replyTo: ActorRef[Event]) extends CommandWithReply

  final case class GetBucketNotifications(replyTo: ActorRef[Event]) extends CommandWithReply

  private final case class UpdateNotifications(notifications: List[Notification],
                                               replyTo: ActorRef[Event]) extends CommandWithReply

  final case class SendNotification(notificationData: NotificationData) extends Command

  private final case class SendNotificationToDestination(destinationType: DestinationType,
                                                         destinationName: String,
                                                         message: String) extends Command

  private final case object Standby extends Command

  private final case object Shutdown extends Command

}
