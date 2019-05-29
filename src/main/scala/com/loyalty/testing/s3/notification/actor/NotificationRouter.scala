package com.loyalty.testing.s3.notification.actor

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props, Terminated}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import com.loyalty.testing.s3.Settings
import com.loyalty.testing.s3.notification.{DestinationType, NotificationData, NotificationMeta}
import com.loyalty.testing.s3.repositories.FileStore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class NotificationRouter(fileStore: FileStore)(implicit settings: Settings)
  extends Actor
    with ActorLogging {

  import DestinationType._
  import NotificationActor._
  import NotificationRouter._

  private val routerState = new RouterState(context)

  override def preStart(): Unit = {
    super.preStart()
    context.setReceiveTimeout(Duration.Undefined)
  }

  override def receive: Receive = {
    case SendNotification(notificationData) =>
      fileStore.get(notificationData.bucketName) match {
        case Some(bucketMetadata) =>
          val notifications = bucketMetadata.notifications
          if (notifications.nonEmpty) {
            notifications
              .foreach {
                notification =>
                  val prefixMatch = notification.prefix.exists(notificationData.key.startsWith) || notification.prefix.isEmpty
                  val suffixMatch = notification.suffix.exists(notificationData.key.endsWith) || notification.suffix.isEmpty
                  if (prefixMatch && suffixMatch) {
                    self ! SendNotificationToDestination(NotificationMeta(notification), notificationData)
                  } else {
                    val argArray = ArrayBuffer.empty[String]
                    val msgArray = ArrayBuffer.empty[String]
                    if(!prefixMatch) {
                      argArray += notificationData.key
                      argArray += notification.prefix.getOrElse("")
                      msgArray += "Key prefix does not match key = {}, prefix = {}"
                    }
                    if(!suffixMatch) {
                      argArray += notificationData.key
                      argArray += notification.suffix.getOrElse("")
                      msgArray += "Key suffix does not match key = {}, suffix = {}"
                    }
                    argArray += notification.bucketName
                    val msg = s"${msgArray.mkString(", ")}, not sending notification for bucket: {}"
                    log.warning(msg, argArray.toArray)
                  }
              }
          } else {
            log.warning("No notification is setup for bucket: {}", notificationData.bucketName)
          }

        case None =>
          log.warning("Unable to get bucket meta data for bucket: {}, skipping notification",
            notificationData.bucketName)
      }

    case SendNotificationToDestination(notificationMeta, notificationData)
      if notificationMeta.destinationType == Sqs =>
      routerState.router.route(SqsNotification(notificationMeta, notificationData), sender())

    case SendNotificationToDestination(notificationMeta, notificationData)
      if notificationMeta.destinationType == Sns =>
      routerState.router.route(SnsNotification(notificationMeta, notificationData), sender())

    case Terminated(a) => routerState.removeRoutee(a)

    case msg => log.warning("Unhandled message: {}", msg)
  }
}

object NotificationRouter {

  def props(fileStore: FileStore)
           (implicit settings: Settings): Props = Props(new NotificationRouter(fileStore))

  case class SendNotification(notificationData: NotificationData)

  private case class SendNotificationToDestination(notificationMeta: NotificationMeta,
                                                   notificationData: NotificationData)

  private class RouterState(context: ActorContext)(implicit settings: Settings) {
    private var _router: Router = {
      val routees = Vector.fill(5) {
        val r = context.actorOf(NotificationActor.props())
        context.watch(r)
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }

    def router: Router = _router

    def removeRoutee(actorRef: ActorRef): Unit = {
      _router = _router.removeRoutee(actorRef)
      val r = context.actorOf(NotificationActor.props())
      context.watch(r)
      _router = _router.addRoutee(r)
    }
  }

}
